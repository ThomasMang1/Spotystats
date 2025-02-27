import psycopg2
import json
import os
import glob

# Connexion à PostgreSQL
def connect_to_db():
    conn = psycopg2.connect(
        dbname="spotify_history",  # Nom de la base de données
        user="postgres",           # Votre nom d'utilisateur PostgreSQL
        password="admin",          # Votre mot de passe PostgreSQL
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
def import_data_from_json(conn, folder_path):
    cur = conn.cursor()
    json_files = glob.glob(os.path.join(folder_path, "*.json"))  # Récupère tous les fichiers JSON du dossier
    
    for json_file in json_files:
        print(f"Importation du fichier : {json_file}")
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            for record in data:
                offline_timestamp = record.get('offline_timestamp')
                if offline_timestamp:  # Conversion si non null
                    offline_timestamp = offline_timestamp / 1000

                record['offline_timestamp'] = offline_timestamp

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

# Écrire le classement des artistes dans un fichier texte
def save_top_artists_to_file(conn, output_file):
    cur = conn.cursor()
    cur.execute("""
        SELECT master_metadata_album_artist_name, COUNT(*) as play_count
        FROM spotify_history
        WHERE master_metadata_album_artist_name IS NOT NULL
        GROUP BY master_metadata_album_artist_name
        ORDER BY play_count DESC
    """)
    results = cur.fetchall()
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for artist, count in results:
            f.write(f"{artist}: {count} écoutes\n")
    
    print(f"Classement des artistes enregistré dans {output_file}")
    cur.close()

# Fonction principale
def main():
    # Dossier contenant les fichiers JSON
    folder_path = 'Spotify Extended Streaming History/'
    output_file = 'ArtistsRanking.txt'  # Fichier de sortie pour les résultats

    # Connexion à la base de données
    conn = connect_to_db()

    # Créer la table
    # create_table(conn)

    # Importer les données JSON de tous les fichiers du dossier
    # import_data_from_json(conn, folder_path)

    # Sauvegarder le classement des artistes dans un fichier texte
    save_top_artists_to_file(conn, output_file)

    # Fermer la connexion
    conn.close()

if __name__ == "__main__":
    main()
