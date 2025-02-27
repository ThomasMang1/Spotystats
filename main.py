import psycopg2
import json

# Connexion à PostgreSQL
def connect_to_db():
    conn = psycopg2.connect(
        dbname="spotify_history",  # Nom de la base de données
        user="postgres",      # Votre nom d'utilisateur PostgreSQL
        password="admin",  # Votre mot de passe PostgreSQL
        host="localhost"           # Hôte de la base de données
    )
    return conn

# Créer la table spotify_history
def create_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS spotify_history (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMP,
            platform TEXT,
            ms_played INTEGER,
            conn_country TEXT,
            ip_addr TEXT,
            master_metadata_track_name TEXT,
            master_metadata_album_artist_name TEXT,
            master_metadata_album_album_name TEXT,
            spotify_track_uri TEXT,
            episode_name TEXT,
            episode_show_name TEXT,
            spotify_episode_uri TEXT,
            reason_start TEXT,
            reason_end TEXT,
            shuffle BOOLEAN,
            skipped BOOLEAN,
            offline BOOLEAN,
            offline_timestamp TIMESTAMP,
            incognito_mode BOOLEAN
        )
    """)
    conn.commit()
    cur.close()

# Importer les données JSON dans la table
def import_data_from_json(conn, json_file):
    cur = conn.cursor()
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        for record in data:
            # Conversion de offline_timestamp en secondes pour la base de données
            offline_timestamp = record['offline_timestamp'] / 1000 if record.get('offline_timestamp') else None
            
            # Ajouter la valeur convertie dans le dictionnaire `record`
            record['offline_timestamp'] = offline_timestamp

            # Insérer les données dans la table
            cur.execute("""
                INSERT INTO spotify_history (
                    ts, platform, ms_played, conn_country, ip_addr,
                    master_metadata_track_name, master_metadata_album_artist_name,
                    master_metadata_album_album_name, spotify_track_uri,
                    episode_name, episode_show_name, spotify_episode_uri,
                    reason_start, reason_end, shuffle, skipped, offline,
                    offline_timestamp, incognito_mode
                ) VALUES (
                    %(ts)s, %(platform)s, %(ms_played)s, %(conn_country)s, %(ip_addr)s,
                    %(master_metadata_track_name)s, %(master_metadata_album_artist_name)s,
                    %(master_metadata_album_album_name)s, %(spotify_track_uri)s,
                    %(episode_name)s, %(episode_show_name)s, %(spotify_episode_uri)s,
                    %(reason_start)s, %(reason_end)s, %(shuffle)s, %(skipped)s, %(offline)s,
                    to_timestamp(%(offline_timestamp)s), %(incognito_mode)s
                )
            """, record)
    conn.commit()
    cur.close()

# Afficher le nombre d'écoutes par artiste classés par ordre décroissant
def get_top_artists(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT master_metadata_album_artist_name, COUNT(*) as play_count
        FROM spotify_history
        WHERE master_metadata_album_artist_name IS NOT NULL
        GROUP BY master_metadata_album_artist_name
        ORDER BY play_count DESC
    """)
    results = cur.fetchall()
    for artist, count in results:
        print(f"{artist}: {count} écoutes")
    cur.close()

# Fonction principale
def main():
    # Chemin vers votre fichier JSON
    json_file = 'Spotify Extended Streaming History\Streaming_History_Audio_2017-2019_0.json'

    # Connexion à la base de données
    conn = connect_to_db()

    # Créer la table
    create_table(conn)

    # Importer les données JSON
    import_data_from_json(conn, json_file)

    # Afficher le nombre d'écoutes par artiste
    print("Nombre d'écoutes par artiste (classement décroissant) :")
    get_top_artists(conn)

    # Fermer la connexion
    conn.close()

if __name__ == "__main__":
    main()
