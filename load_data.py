import csv
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'baseball_stats.db')

def clean_value(value):
    """Convert empty strings to None for proper NULL handling"""
    return None if value == '' else value

def load_pitcher_csv(csv_file, year, conn, cursor):
    cursor.execute('DELETE FROM pitcher_stats WHERE year = ?', (year,))
    print(f"Loading pitcher data for year {year}...")

    headers = ['Rk', 'Player', 'Age', 'Team', 'Lg', 'WAR', 'W', 'L', 'W-L%', 'ERA',
               'G', 'GS', 'GF', 'CG', 'SHO', 'SV', 'IP', 'H', 'R', 'ER', 'HR', 'BB',
               'IBB', 'SO', 'HBP', 'BK', 'WP', 'BF', 'ERA+', 'FIP', 'WHIP', 'H9',
               'HR9', 'BB9', 'SO9', 'SO/BB', 'Awards', 'Player-additional']

    with open(csv_file, 'r', encoding='utf-8') as f:
        first_line = f.readline().strip()
        f.seek(0)

        if first_line and first_line[0].isdigit():
            reader = csv.reader(f)
        else:
            reader = csv.DictReader(f)
            headers = None

        for row_data in reader:
            if headers:
                row = dict(zip(headers, row_data))
            else:
                row = row_data

            cursor.execute('''
                INSERT INTO pitcher_stats (
                    year, rk, player, age, team, lg, war, w, l, w_l_pct, era,
                    g, gs, gf, cg, sho, sv, ip, h, r, er, hr, bb, ibb, so,
                    hbp, bk, wp, bf, era_plus, fip, whip, h9, hr9, bb9, so9,
                    so_bb, awards, player_additional
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?
                )
            ''', (
                year,
                clean_value(row['Rk']),
                clean_value(row['Player']),
                clean_value(row['Age']),
                clean_value(row['Team']),
                clean_value(row['Lg']),
                clean_value(row['WAR']),
                clean_value(row['W']),
                clean_value(row['L']),
                clean_value(row['W-L%']),
                clean_value(row['ERA']),
                clean_value(row['G']),
                clean_value(row['GS']),
                clean_value(row['GF']),
                clean_value(row['CG']),
                clean_value(row['SHO']),
                clean_value(row['SV']),
                clean_value(row['IP']),
                clean_value(row['H']),
                clean_value(row['R']),
                clean_value(row['ER']),
                clean_value(row['HR']),
                clean_value(row['BB']),
                clean_value(row['IBB']),
                clean_value(row['SO']),
                clean_value(row['HBP']),
                clean_value(row['BK']),
                clean_value(row['WP']),
                clean_value(row['BF']),
                clean_value(row['ERA+']),
                clean_value(row['FIP']),
                clean_value(row['WHIP']),
                clean_value(row['H9']),
                clean_value(row['HR9']),
                clean_value(row['BB9']),
                clean_value(row['SO9']),
                clean_value(row['SO/BB']),
                clean_value(row['Awards']),
                clean_value(row['Player-additional'])
            ))

    count = cursor.execute('SELECT COUNT(*) FROM pitcher_stats WHERE year = ?', (year,)).fetchone()[0]
    print(f"Loaded {count} pitcher records for {year}")

def load_hitter_csv(csv_file, year, conn, cursor):
    cursor.execute('DELETE FROM hitter_stats WHERE year = ?', (year,))
    print(f"Loading hitter data for year {year}...")

    headers = ['Rk', 'Player', 'Age', 'Team', 'Lg', 'WAR', 'G', 'PA', 'AB', 'R', 'H',
               '2B', '3B', 'HR', 'RBI', 'SB', 'CS', 'BB', 'SO', 'BA', 'OBP', 'SLG',
               'OPS', 'OPS+', 'rOBA', 'Rbat+', 'TB', 'GIDP', 'HBP', 'SH', 'SF', 'IBB',
               'Pos', 'Awards', 'Player-additional']

    with open(csv_file, 'r', encoding='utf-8') as f:
        first_line = f.readline().strip()
        f.seek(0)

        if first_line and first_line[0].isdigit():
            reader = csv.reader(f)
        else:
            reader = csv.DictReader(f)
            headers = None

        for row_data in reader:
            if headers:
                row = dict(zip(headers, row_data))
            else:
                row = row_data
            cursor.execute('''
                INSERT INTO hitter_stats (
                    year, rk, player, age, team, lg, war, g, pa, ab, r, h,
                    doubles, triples, hr, rbi, sb, cs, bb, so, ba, obp, slg, ops,
                    ops_plus, roba, rbat_plus, tb, gidp, hbp, sh, sf, ibb, pos,
                    awards, player_additional
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?
                )
            ''', (
                year,
                clean_value(row['Rk']),
                clean_value(row['Player']),
                clean_value(row['Age']),
                clean_value(row['Team']),
                clean_value(row['Lg']),
                clean_value(row['WAR']),
                clean_value(row['G']),
                clean_value(row['PA']),
                clean_value(row['AB']),
                clean_value(row['R']),
                clean_value(row['H']),
                clean_value(row['2B']),
                clean_value(row['3B']),
                clean_value(row['HR']),
                clean_value(row['RBI']),
                clean_value(row['SB']),
                clean_value(row['CS']),
                clean_value(row['BB']),
                clean_value(row['SO']),
                clean_value(row['BA']),
                clean_value(row['OBP']),
                clean_value(row['SLG']),
                clean_value(row['OPS']),
                clean_value(row['OPS+']),
                clean_value(row['rOBA']),
                clean_value(row['Rbat+']),
                clean_value(row['TB']),
                clean_value(row['GIDP']),
                clean_value(row['HBP']),
                clean_value(row['SH']),
                clean_value(row['SF']),
                clean_value(row['IBB']),
                clean_value(row['Pos']),
                clean_value(row['Awards']),
                clean_value(row['Player-additional'])
            ))

    count = cursor.execute('SELECT COUNT(*) FROM hitter_stats WHERE year = ?', (year,)).fetchone()[0]
    print(f"Loaded {count} hitter records for {year}")

def load_csv_to_db(csv_file, year):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    if 'pitcher' in csv_file.lower():
        load_pitcher_csv(csv_file, year, conn, cursor)
    elif 'hitter' in csv_file.lower():
        load_hitter_csv(csv_file, year, conn, cursor)
    else:
        print(f"Error: Could not determine file type from filename '{csv_file}'")
        print("Filename should contain 'pitcher' or 'hitter'")
        conn.close()
        return

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    import sys
    import re

    if len(sys.argv) < 2:
        print("Usage: python load_data.py <csv_file> [year]")
        print("Example: python load_data.py pitcher_stats_2024.csv 2024")
        print("If year is not provided, will try to extract from filename")
        sys.exit(1)

    csv_file = sys.argv[1]

    # Try to get year from command line or filename
    if len(sys.argv) >= 3:
        year = int(sys.argv[2])
    else:
        # Try to extract year from filename (e.g., "pitcher_stats_2024.csv")
        match = re.search(r'(202[0-9])', csv_file)
        if match:
            year = int(match.group(1))
        else:
            print("Error: Could not determine year. Please provide year as second argument.")
            print("Example: python load_data.py pitcher_stats_2025.csv 2024")
            sys.exit(1)

    load_csv_to_db(csv_file, year)
