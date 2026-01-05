import csv
import sqlite3
import os
import glob

DB_PATH = os.path.join(os.path.dirname(__file__), 'baseball_stats.db')

def clean_value(value):
    """Convert empty strings to None for proper NULL handling"""
    return None if value == '' else value

def load_team_hitter_csv(csv_file, year):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('DELETE FROM team_hitter_stats WHERE year = ?', (year,))
    print(f"Loading team hitter data for year {year}...")

    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            cursor.execute('''
                INSERT INTO team_hitter_stats (
                    year, tm, bat_count, bat_age, r_per_g, g, pa, ab, r, h,
                    doubles, triples, hr, rbi, sb, cs, bb, so, ba, obp, slg, ops,
                    ops_plus, tb, gdp, hbp, sh, sf, ibb, lob
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?
                )
            ''', (
                year,
                clean_value(row['Tm']),
                clean_value(row['#Bat']),
                clean_value(row['BatAge']),
                clean_value(row['R/G']),
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
                clean_value(row['TB']),
                clean_value(row['GDP']),
                clean_value(row['HBP']),
                clean_value(row['SH']),
                clean_value(row['SF']),
                clean_value(row['IBB']),
                clean_value(row['LOB'])
            ))

    conn.commit()
    count = cursor.execute('SELECT COUNT(*) FROM team_hitter_stats WHERE year = ?', (year,)).fetchone()[0]
    print(f"Loaded {count} team hitter records for {year}")

    cursor.close()
    conn.close()

def load_team_pitcher_csv(csv_file, year):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('DELETE FROM team_pitcher_stats WHERE year = ?', (year,))
    print(f"Loading team pitcher data for year {year}...")

    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            cursor.execute('''
                INSERT INTO team_pitcher_stats (
                    year, tm, pitcher_count, p_age, ra_per_g, w, l, w_l_pct, era, g,
                    gs, gf, cg, t_sho, c_sho, sv, ip, h, r, er, hr, bb, ibb, so,
                    hbp, bk, wp, bf, era_plus, fip, whip, h9, hr9, bb9, so9, so_w, lob
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            ''', (
                year,
                clean_value(row['Tm']),
                clean_value(row['#P']),
                clean_value(row['PAge']),
                clean_value(row['RA/G']),
                clean_value(row['W']),
                clean_value(row['L']),
                clean_value(row['W-L%']),
                clean_value(row['ERA']),
                clean_value(row['G']),
                clean_value(row['GS']),
                clean_value(row['GF']),
                clean_value(row['CG']),
                clean_value(row['tSho']),
                clean_value(row['cSho']),
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
                clean_value(row['SO/W']),
                clean_value(row['LOB'])
            ))

    conn.commit()
    count = cursor.execute('SELECT COUNT(*) FROM team_pitcher_stats WHERE year = ?', (year,)).fetchone()[0]
    print(f"Loaded {count} team pitcher records for {year}")

    cursor.close()
    conn.close()

if __name__ == '__main__':
    # Load all team hitting stats
    for csv_file in sorted(glob.glob('team_hitting_stats_*.csv')):
        year = int(csv_file.split('_')[-1].replace('.csv', ''))
        load_team_hitter_csv(csv_file, year)

    # Load all team pitching stats
    for csv_file in sorted(glob.glob('team_pitching_stats_*.csv')):
        year = int(csv_file.split('_')[-1].replace('.csv', ''))
        load_team_pitcher_csv(csv_file, year)

    print("\nAll team stats loaded successfully!")
