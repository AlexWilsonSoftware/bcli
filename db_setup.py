import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'baseball_stats.db')

def create_tables():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pitcher_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            rk INTEGER,
            player TEXT,
            age INTEGER,
            team TEXT,
            lg TEXT,
            war REAL,
            w INTEGER,
            l INTEGER,
            w_l_pct REAL,
            era REAL,
            g INTEGER,
            gs INTEGER,
            gf INTEGER,
            cg INTEGER,
            sho INTEGER,
            sv INTEGER,
            ip REAL,
            h INTEGER,
            r INTEGER,
            er INTEGER,
            hr INTEGER,
            bb INTEGER,
            ibb INTEGER,
            so INTEGER,
            hbp INTEGER,
            bk INTEGER,
            wp INTEGER,
            bf INTEGER,
            era_plus INTEGER,
            fip REAL,
            whip REAL,
            h9 REAL,
            hr9 REAL,
            bb9 REAL,
            so9 REAL,
            so_bb REAL,
            awards TEXT,
            player_additional TEXT
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_pitcher_player_lower ON pitcher_stats (player COLLATE NOCASE)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_pitcher_year ON pitcher_stats (year)
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS hitter_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            rk INTEGER,
            player TEXT,
            age INTEGER,
            team TEXT,
            lg TEXT,
            war REAL,
            g INTEGER,
            pa INTEGER,
            ab INTEGER,
            r INTEGER,
            h INTEGER,
            doubles INTEGER,
            triples INTEGER,
            hr INTEGER,
            rbi INTEGER,
            sb INTEGER,
            cs INTEGER,
            bb INTEGER,
            so INTEGER,
            ba REAL,
            obp REAL,
            slg REAL,
            ops REAL,
            ops_plus INTEGER,
            roba REAL,
            rbat_plus INTEGER,
            tb INTEGER,
            gidp INTEGER,
            hbp INTEGER,
            sh INTEGER,
            sf INTEGER,
            ibb INTEGER,
            pos TEXT,
            awards TEXT,
            player_additional TEXT
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_hitter_player_lower ON hitter_stats (player COLLATE NOCASE)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_hitter_year ON hitter_stats (year)
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS team_hitter_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            tm TEXT,
            bat_count INTEGER,
            bat_age REAL,
            r_per_g REAL,
            g INTEGER,
            pa INTEGER,
            ab INTEGER,
            r INTEGER,
            h INTEGER,
            doubles INTEGER,
            triples INTEGER,
            hr INTEGER,
            rbi INTEGER,
            sb INTEGER,
            cs INTEGER,
            bb INTEGER,
            so INTEGER,
            ba REAL,
            obp REAL,
            slg REAL,
            ops REAL,
            ops_plus INTEGER,
            tb INTEGER,
            gdp INTEGER,
            hbp INTEGER,
            sh INTEGER,
            sf INTEGER,
            ibb INTEGER,
            lob INTEGER
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_team_hitter_year ON team_hitter_stats (year)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_team_hitter_tm ON team_hitter_stats (tm)
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS team_pitcher_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            tm TEXT,
            pitcher_count INTEGER,
            p_age REAL,
            ra_per_g REAL,
            w INTEGER,
            l INTEGER,
            w_l_pct REAL,
            era REAL,
            g INTEGER,
            gs INTEGER,
            gf INTEGER,
            cg INTEGER,
            t_sho INTEGER,
            c_sho INTEGER,
            sv INTEGER,
            ip REAL,
            h INTEGER,
            r INTEGER,
            er INTEGER,
            hr INTEGER,
            bb INTEGER,
            ibb INTEGER,
            so INTEGER,
            hbp INTEGER,
            bk INTEGER,
            wp INTEGER,
            bf INTEGER,
            era_plus INTEGER,
            fip REAL,
            whip REAL,
            h9 REAL,
            hr9 REAL,
            bb9 REAL,
            so9 REAL,
            so_w REAL,
            lob INTEGER
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_team_pitcher_year ON team_pitcher_stats (year)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_team_pitcher_tm ON team_pitcher_stats (tm)
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS batter_pitcher_matchups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batter_name TEXT NOT NULL,
            batter_mlb_id INTEGER NOT NULL,
            pitcher_name TEXT NOT NULL,
            pitcher_mlb_id INTEGER NOT NULL,
            year TEXT,
            games INTEGER,
            pa INTEGER,
            ab INTEGER,
            h INTEGER,
            doubles INTEGER,
            triples INTEGER,
            hr INTEGER,
            rbi INTEGER,
            bb INTEGER,
            so INTEGER,
            hbp INTEGER,
            ibb INTEGER,
            ba REAL,
            obp REAL,
            slg REAL,
            ops REAL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(batter_name, pitcher_name, year)
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_matchup_lookup ON batter_pitcher_matchups (batter_name, pitcher_name)
    ''')

    conn.commit()
    print(f"Database created at: {DB_PATH}")
    print("Tables 'pitcher_stats', 'hitter_stats', 'team_pitcher_stats', 'team_hitter_stats', and 'batter_pitcher_matchups' created successfully")

    cursor.close()
    conn.close()

if __name__ == '__main__':
    create_tables()
