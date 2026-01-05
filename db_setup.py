import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'baseball_stats.db')

def create_tables():
    """Create the pitcher_stats table"""
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
        CREATE INDEX IF NOT EXISTS idx_player_lower ON pitcher_stats (player COLLATE NOCASE)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_year ON pitcher_stats (year)
    ''')

    conn.commit()
    print(f"Database created at: {DB_PATH}")
    print("Table 'pitcher_stats' created successfully")

    cursor.close()
    conn.close()

if __name__ == '__main__':
    create_tables()
