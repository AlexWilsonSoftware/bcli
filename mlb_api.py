import requests
import statsapi
from datetime import datetime

def lookup_player(name):
    """Look up player by name using MLB Stats API

    Returns:
        dict with keys: 'id', 'fullName', 'position' (e.g., 'P', 'TWP', etc.)
        None if player not found
    """
    try:
        players = statsapi.lookup_player(name)
        if not players:
            return None

        # Return the first match
        player = players[0]
        position_code = player.get('primaryPosition', {}).get('abbreviation', 'Unknown')

        return {
            'id': player['id'],
            'fullName': player['fullName'],
            'position': position_code
        }
    except Exception as e:
        print(f"Error looking up player '{name}': {e}")
        return None

def fetch_batter_vs_pitcher_stats(batter_id, pitcher_id):
    """Fetch matchup stats from MLB Stats API

    Returns:
        List of dicts, one per year plus one 'career' total:
        [
            {'year': '2024', 'games': 3, 'pa': 12, 'ab': 11, 'h': 2, ...},
            {'year': '2023', 'games': 1, ...},
            {'year': 'career', 'games': 7, ...}
        ]
        Returns empty list if no matchups found or API error
    """
    try:
        url = f"https://statsapi.mlb.com/api/v1/people/{batter_id}/stats"
        params = {
            'stats': 'vsPlayer',
            'opposingPlayerId': pitcher_id,
            'group': 'hitting'
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()

        if 'stats' not in data or len(data['stats']) == 0:
            return []

        results = []

        # Process each stat group (vsPlayer by year, vsPlayerTotal)
        for stat_group in data['stats']:
            stat_type = stat_group['type']['displayName']

            for split in stat_group.get('splits', []):
                stat = split.get('stat', {})

                # Determine if this is a yearly split or career total
                if 'season' in split:
                    year = str(split['season'])
                elif stat_type == 'vsPlayerTotal':
                    year = 'career'
                else:
                    continue

                # Map API fields to our database fields
                # Parse batting average stats - API returns them as ".364" or "1.000"
                def parse_avg_stat(value):
                    if not value or value == '-.--':
                        return 0.0
                    try:
                        # If starts with period, add leading zero
                        if value.startswith('.'):
                            return float('0' + value)
                        return float(value)
                    except (ValueError, TypeError):
                        return 0.0

                matchup_data = {
                    'year': year,
                    'games': stat.get('gamesPlayed', 0),
                    'pa': stat.get('plateAppearances', 0),
                    'ab': stat.get('atBats', 0),
                    'h': stat.get('hits', 0),
                    'doubles': stat.get('doubles', 0),
                    'triples': stat.get('triples', 0),
                    'hr': stat.get('homeRuns', 0),
                    'rbi': stat.get('rbi', 0),
                    'bb': stat.get('baseOnBalls', 0),
                    'so': stat.get('strikeOuts', 0),
                    'hbp': stat.get('hitByPitch', 0),
                    'ibb': stat.get('intentionalWalks', 0),
                    'ba': parse_avg_stat(stat.get('avg')),
                    'obp': parse_avg_stat(stat.get('obp')),
                    'slg': parse_avg_stat(stat.get('slg')),
                    'ops': parse_avg_stat(stat.get('ops')),
                }

                results.append(matchup_data)

        return results

    except requests.exceptions.RequestException as e:
        print(f"Error fetching matchup data from API: {e}")
        return []
    except Exception as e:
        print(f"Error processing matchup data: {e}")
        return []

def cache_matchup(cursor, batter_name, batter_id, pitcher_name, pitcher_id, stats_list):
    """Cache matchup stats in database

    Args:
        cursor: Database cursor
        batter_name: Full name of batter
        batter_id: MLB ID of batter
        pitcher_name: Full name of pitcher
        pitcher_id: MLB ID of pitcher
        stats_list: List of stat dicts from fetch_batter_vs_pitcher_stats()
    """
    try:
        for stats in stats_list:
            cursor.execute('''
                INSERT OR REPLACE INTO batter_pitcher_matchups (
                    batter_name, batter_mlb_id, pitcher_name, pitcher_mlb_id, year,
                    games, pa, ab, h, doubles, triples, hr, rbi,
                    bb, so, hbp, ibb, ba, obp, slg, ops, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                batter_name, batter_id, pitcher_name, pitcher_id, stats['year'],
                stats['games'], stats['pa'], stats['ab'], stats['h'],
                stats['doubles'], stats['triples'], stats['hr'], stats['rbi'],
                stats['bb'], stats['so'], stats['hbp'], stats['ibb'],
                stats['ba'], stats['obp'], stats['slg'], stats['ops']
            ))

        cursor.connection.commit()

    except Exception as e:
        print(f"Error caching matchup data: {e}")
        cursor.connection.rollback()

def get_cached_matchup(cursor, batter_name, pitcher_name):
    """Get cached matchup from database

    Returns:
        List of dicts with matchup stats, or empty list if not cached
    """
    try:
        cursor.execute('''
            SELECT year, games, pa, ab, h, doubles, triples, hr, rbi,
                   bb, so, hbp, ibb, ba, obp, slg, ops
            FROM batter_pitcher_matchups
            WHERE batter_name = ? AND pitcher_name = ?
            ORDER BY CASE WHEN year = 'career' THEN 9999 ELSE CAST(year AS INTEGER) END
        ''', (batter_name, pitcher_name))

        rows = cursor.fetchall()

        if not rows:
            return []

        results = []
        for row in rows:
            results.append({
                'year': row[0],
                'games': row[1],
                'pa': row[2],
                'ab': row[3],
                'h': row[4],
                'doubles': row[5],
                'triples': row[6],
                'hr': row[7],
                'rbi': row[8],
                'bb': row[9],
                'so': row[10],
                'hbp': row[11],
                'ibb': row[12],
                'ba': row[13],
                'obp': row[14],
                'slg': row[15],
                'ops': row[16],
            })

        return results

    except Exception as e:
        print(f"Error retrieving cached matchup: {e}")
        return []
