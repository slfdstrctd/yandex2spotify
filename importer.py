import json
import argparse
import logging
from base64 import b64encode
from time import sleep
from datetime import datetime

import spotipy
from PIL import Image
from requests.exceptions import ReadTimeout
from spotipy.exceptions import SpotifyException
from spotipy.oauth2 import SpotifyOAuth
from yandex_music import Client, Artist

REDIRECT_URI = 'https://open.spotify.com'
MAX_REQUEST_RETRIES = 5

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def encode_file_base64_jpeg(filename):
    img = Image.open(filename)
    if img.format != 'JPEG':
        img.convert('RGB').save(filename, 'JPEG')

    with open(filename, 'rb') as f:
        return b64encode(f.read())


def handle_spotify_exception(func):
    def wrapper(*args, **kwargs):
        retry = 1
        while True:
            try:
                return func(*args, **kwargs)
            except SpotifyException as exception:
                if exception.http_status != 429:
                    raise exception

                if 'retry-after' in exception.headers:
                    sleep(int(exception.headers['retry-after']) + 1)
            except ReadTimeout as exception:
                logger.info(f'Read timed out. Retrying #{retry}...')

                if retry > MAX_REQUEST_RETRIES:
                    logger.info('Max retries reached.')
                    raise exception

                logger.info('Trying again...')
                retry += 1

    return wrapper


class NotFoundException(SpotifyException):
    def __init__(self, item_name):
        self.item_name = item_name


class Importer:
    def __init__(self, spotify_client, yandex_client: Client, ignore_list, strict_search):
        self.spotify_client = spotify_client
        self.yandex_client = yandex_client

        self._importing_items = {
            'likes': self.import_likes,
            'playlists': self.import_playlists,
            'albums': self.import_albums,
            'artists': self.import_artists
        }

        for item in ignore_list:
            del self._importing_items[item]

        self._strict_search = strict_search

        self.user = handle_spotify_exception(spotify_client.me)()['id']
        logger.info(f'User ID: {self.user}')

        self.not_imported = {}

    def _import_item(self, item):
        # if the item is a string, it is a query from the JSON file
        if isinstance(item, str):
            query = item
            item_name = item
            type_ = 'track'  # Default type for string items
            artists = []  # Default artists for string items
        # else it is an object from Yandex
        else:
            type_ = item.__class__.__name__.casefold()
            item_name = item.name if isinstance(item, Artist) else f'{", ".join([artist.name for artist in item.artists])} - {item.title}'
            artists = item.artists if not isinstance(item, Artist) else []  # Artists for Yandex items

            # A workaround for when track name is too long (100+ characters) there is an exception happening
            # because spotify API can not process it.
            if len(item_name) > 100:
                item_name = item_name[:100]
                logger.info('Name too long... Trimming to 100 characters. May affect search accuracy')

            query = item_name.replace('- ', '')

        found_items = handle_spotify_exception(self.spotify_client.search)(query, type=type_)[f'{type_}s']['items']
        logger.info(f'Importing {type_}: {item_name}...')

        if not self._strict_search and not isinstance(item, Artist) and not len(found_items) and len(artists) > 1:
            query = f'{artists[0].name} {item.title}'
            found_items = handle_spotify_exception(self.spotify_client.search)(query, type=type_)[f'{type_}s']['items']

        logger.info(f'Searching "{query}"...')

        if not len(found_items):
            raise NotFoundException(item_name)

        return found_items[0]['id']

    def _add_items_to_spotify(self, items, not_imported_section, save_items_callback, api_method, playlist_id=None):
        spotify_items = []
        
        # Process items to get Spotify IDs while maintaining order
        for item in items:
            # if True:
            if not item.available: # process hidden tracks
                try:
                    spotify_id = self._import_item(item)
                    if spotify_id is None:
                        logger.warning('Item ID is None, skipping...')
                        continue
                    spotify_items.append(spotify_id)
                    logger.info('OK')
                except NotFoundException as exception:
                    not_imported_section.append(exception.item_name)
                    logger.warning('NO')
                except SpotifyException:
                    not_imported_section.append(item.title if hasattr(item, 'title') else str(item))
                    logger.warning('NO')

        if not spotify_items:
            logger.info('No valid Spotify items to add.')
            return
        
        logger.info(f"Adding {len(spotify_items)} items one by one with sleep timeout...")
        
        # Process tracks one by one
        for i, item_id in enumerate(spotify_items):
            logger.info(f"Adding item {i+1}/{len(spotify_items)}: {item_id}")
            
            try:
                if api_method == "tracks":
                    handle_spotify_exception(self.spotify_client.current_user_saved_tracks_add)([item_id])
                elif api_method == "playlist_tracks":
                    # Add at the end of the playlist, one by one
                    handle_spotify_exception(self.spotify_client.user_playlist_add_tracks)(
                        self.user, 
                        playlist_id, 
                        [item_id]
                    )
                elif api_method == "albums":
                    handle_spotify_exception(self.spotify_client.current_user_saved_albums_add)([item_id])
                elif api_method == "artists":
                    handle_spotify_exception(self.spotify_client.user_follow_artists)([item_id])
                
                logger.info(f"Item {i+1} successfully added")
                
                # Add sleep timeout between requests to avoid rate limiting and ensure proper ordering
                sleep(1)  # 1 second timeout between adding tracks
                
            except Exception as e:
                logger.error(f"Error adding item {item_id}: {str(e)}")
                not_imported_section.append(item_id)

    def import_likes(self):
        self.not_imported['Likes'] = []

        # Get all liked tracks
        likes_tracks = self.yandex_client.users_likes_tracks().tracks
        
        # Sort by timestamp (oldest first)
        if likes_tracks and hasattr(likes_tracks[0], 'timestamp'):
            logger.info("Sorting liked tracks by timestamp (oldest first)...")
            likes_tracks.sort(key=lambda x: x.timestamp if hasattr(x, 'timestamp') else 0)
            
            # Log the sorted order
            for i, track in enumerate(likes_tracks[:5]):
                logger.info(f"Track {i}: {track.id} - Timestamp: {track.timestamp if hasattr(track, 'timestamp') else 'N/A'}")
            
            if len(likes_tracks) > 5:
                logger.info(f"... and {len(likes_tracks) - 5} more tracks")
        
        # Get track IDs in the sorted order
        track_ids = [f'{track.id}:{track.album_id}' for track in likes_tracks if track.album_id]
        
        # Fetch full track details
        tracks = self.yandex_client.tracks(track_ids)
        logger.info(f'Importing {len(tracks)} liked tracks in chronological order...')
        
        # Now add them to Spotify in the same order
        self._add_items_to_spotify(tracks, self.not_imported['Likes'], None, "tracks")

    def import_playlists(self):
        playlists = self.yandex_client.users_playlists_list()
        for playlist in playlists:
            spotify_playlist = handle_spotify_exception(self.spotify_client.user_playlist_create)(self.user, playlist.title)
            spotify_playlist_id = spotify_playlist['id']

            logger.info(f'Importing playlist {playlist.title}...')

            if playlist.cover and hasattr(playlist.cover, 'type') and playlist.cover.type == 'pic':
                filename = f'{playlist.kind}-cover'
                playlist.cover.download(filename, size='400x400')

                handle_spotify_exception(self.spotify_client.playlist_upload_cover_image)(spotify_playlist_id, encode_file_base64_jpeg(filename))

            self.not_imported[playlist.title] = []

            # Fetch playlist tracks
            logger.info(f"Fetching tracks for playlist: {playlist.title}")
            playlist_tracks = playlist.fetch_tracks()
            
            # Sort by timestamp if can
            if playlist_tracks and hasattr(playlist_tracks[0], 'timestamp'):
                logger.info(f"Sorting {len(playlist_tracks)} playlist tracks by timestamp (oldest first)...")
                playlist_tracks.sort(key=lambda x: x.timestamp if hasattr(x, 'timestamp') else 0)
                
                # Log the first few tracks to verify order
                for i, track in enumerate(playlist_tracks[:5]):
                    logger.info(f"Track {i}: ID {track.track_id} - Timestamp: {track.timestamp if hasattr(track, 'timestamp') else 'N/A'}")
                
                if len(playlist_tracks) > 5:
                    logger.info(f"... and {len(playlist_tracks) - 5} more tracks")
            
            # Process track data based on playlist type
            if not playlist.collective:
                # For regular playlists
                tracks = []
                for track_info in playlist_tracks:
                    if hasattr(track_info, 'track') and track_info.track:
                        tracks.append(track_info.track)
                    else:
                        logger.warning(f"Missing track data for track in playlist {playlist.title}")
            elif playlist.collective and playlist_tracks:
                # For collective playlists, maintain the order after sorting
                track_ids = [track.track_id for track in playlist_tracks]
                
                # Log the order of track IDs
                logger.info(f"Fetching tracks in this order: {track_ids[:5]}... (and {len(track_ids)-5} more)")
                
                # Fetch all tracks at once
                all_tracks = self.yandex_client.tracks(track_ids)
                
                # Create a map of tracks by ID
                tracks_map = {str(track.id): track for track in all_tracks}
                
                # Rebuild the tracks list in the original (sorted) order
                tracks = []
                for track_id in track_ids:
                    track = tracks_map.get(str(track_id))
                    if track:
                        tracks.append(track)
                    else:
                        logger.warning(f"Could not find track with ID {track_id} in fetched tracks")
            else:
                tracks = []

            logger.info(f'Processing {len(tracks)} tracks for playlist {playlist.title}')
            
            # Add tracks to Spotify playlist in the correct order
            self._add_items_to_spotify(tracks, self.not_imported[playlist.title], None, "playlist_tracks", spotify_playlist_id)

    def import_albums(self):
        self.not_imported['Albums'] = []

        likes_albums = self.yandex_client.users_likes_albums()
        
        # Sort albums by timestamp (oldest first)
        if likes_albums and hasattr(likes_albums[0], 'timestamp'):
            logger.info("Sorting albums by timestamp (oldest first)...")
            likes_albums.sort(key=lambda x: x.timestamp if hasattr(x, 'timestamp') else 0)
            
            # Log the sorted order
            for i, album in enumerate(likes_albums[:5]):
                logger.info(f"Album {i}: {album.album.id if hasattr(album, 'album') else 'N/A'} - Timestamp: {album.timestamp if hasattr(album, 'timestamp') else 'N/A'}")
            
            if len(likes_albums) > 5:
                logger.info(f"... and {len(likes_albums) - 5} more albums")
        
        albums = [album.album for album in likes_albums if hasattr(album, 'album')]
        logger.info(f'Importing {len(albums)} albums in chronological order...')
        
        # Add to Spotify in the correct order
        self._add_items_to_spotify(albums, self.not_imported['Albums'], None, "albums")

    def import_artists(self):
        self.not_imported['Artists'] = []

        likes_artists = self.yandex_client.users_likes_artists()
        
        # Sort artists by timestamp if can
        if likes_artists and hasattr(likes_artists[0], 'timestamp'):
            logger.info("Sorting artists by timestamp (oldest first)...")
            likes_artists.sort(key=lambda x: x.timestamp if hasattr(x, 'timestamp') else 0)
            
            # Log the sorted order
            for i, artist in enumerate(likes_artists[:5]):
                logger.info(f"Artist {i}: {artist.artist.id if hasattr(artist, 'artist') else 'N/A'} - Timestamp: {artist.timestamp if hasattr(artist, 'timestamp') else 'N/A'}")
            
            if len(likes_artists) > 5:
                logger.info(f"... and {len(likes_artists) - 5} more artists")
        
        artists = [artist.artist for artist in likes_artists if hasattr(artist, 'artist')]
        logger.info(f'Importing {len(artists)} artists in chronological order...')
        
        # Add to Spotify in the correct order
        self._add_items_to_spotify(artists, self.not_imported['Artists'], None, "artists")

    def import_all(self):
        for item in self._importing_items.values():
            item()

        self.print_not_imported()

    def print_not_imported(self):
        logger.error('Not imported items:')
        for section, items in self.not_imported.items():
            logger.info(f'{section}:')
            for item in items:
                logger.info(item)

    def import_from_json(self, file_path):
        with open(file_path, 'r', encoding='UTF-8') as file:
            tracks = json.load(file)

        # If the JSON file has timestamps, sort by those
        if tracks and 'timestamp' in tracks[0]:
            logger.info("Sorting JSON tracks by timestamp (oldest first)...")
            tracks.sort(key=lambda x: x.get('timestamp', 0))
            
            # Log the sorted order
            for i, track in enumerate(tracks[:5]):
                logger.info(f"Track {i}: {track.get('artist', 'Unknown')} - {track.get('track', 'Unknown')} - Timestamp: {track.get('timestamp', 'N/A')}")
            
            if len(tracks) > 5:
                logger.info(f"... and {len(tracks) - 5} more tracks")

        spotify_tracks = []
        not_imported = []

        # Process tracks in order
        for track in tracks:
            query = f'{track["artist"]} {track["track"]}'

            try:
                spotify_track_id = self._import_item(query)
                spotify_tracks.append(spotify_track_id)
                logger.info('OK')
            except NotFoundException as exception:
                not_imported.append(exception.item_name)
                logger.warning('NO')
            except SpotifyException:
                not_imported.append(query)
                logger.warning('NO')

        # Create a new playlist
        playlist_name = 'Imported from JSON'
        playlist = handle_spotify_exception(self.spotify_client.user_playlist_create)(self.user, playlist_name)
        playlist_id = playlist['id']
        
        logger.info(f"Created playlist '{playlist_name}' with ID {playlist_id}")
        
        # Add tracks to the new playlist in chunks, but maintain order
        chunks_list = list(chunks(spotify_tracks, 50))
        logger.info(f"Processing {len(chunks_list)} chunks of tracks for JSON import")
        
        for i, chunk in enumerate(chunks_list):
            logger.info(f"Adding chunk {i+1}/{len(chunks_list)} with {len(chunk)} tracks to playlist")
            handle_spotify_exception(self.spotify_client.user_playlist_add_tracks)(self.user, playlist_id, chunk)
            logger.info(f"Chunk {i+1} successfully added")

        logger.error('Not imported tracks:')
        for track in not_imported:
            logger.info(track)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Creates a playlist for user')
    parser.add_argument('-u', '-s', '--spotify', required=True, help='Username at spotify.com')

    spotify_oauth = parser.add_argument_group('spotify_oauth')
    spotify_oauth.add_argument('--id', required=True, help='Client ID of your Spotify app')
    spotify_oauth.add_argument('--secret', required=True, help='Client Secret of your Spotify app')

    parser.add_argument('-t', '--token', help='Token from music.yandex.com account')

    parser.add_argument('-i', '--ignore', nargs='+', help='Don\'t import some items',
                        choices=['likes', 'playlists', 'albums', 'artists'], default=[])

    parser.add_argument('-T', '--timeout', help='Request timeout for spotify', type=float, default=10)

    parser.add_argument('-S', '--strict-artists-search', help='Search for an exact match of all artists', action='store_true')

    parser.add_argument('-j', '--json-path', help='JSON file to import tracks from')

    arguments = parser.parse_args()

    try:
        auth_manager = SpotifyOAuth(
            client_id=arguments.id,
            client_secret=arguments.secret,
            redirect_uri=REDIRECT_URI,
            scope='playlist-modify-public, user-library-modify, user-follow-modify, ugc-image-upload',
            username=arguments.spotify,
        )

        if arguments.token is None and arguments.json_path is None:
            raise ValueError('Either the -t (token) or -j (json_path) argument must be specified.')

        spotify_client_ = spotipy.Spotify(auth_manager=auth_manager, requests_timeout=arguments.timeout)
        yandex_client_ = None

        if arguments.token:
            yandex_client_ = Client(arguments.token)
            yandex_client_.init()

        importer_instance = Importer(spotify_client_, yandex_client_, arguments.ignore, arguments.strict_artists_search)

        if arguments.json_path:
            importer_instance.import_from_json(arguments.json_path)
        else:
            importer_instance.import_all()
    except Exception as e:
        logger.error(f'An unexpected error occurred: {str(e)}')