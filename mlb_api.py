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

def fetch_platoon_splits(player_id, player_type, year=None, all_years=False):
    """Fetch platoon splits (vs LHB/RHB for pitchers, vs LHP/RHP for hitters)

    Args:
        player_id: MLB player ID
        player_type: 'pitcher' or 'hitter'
        year: Optional year (defaults to career)
        all_years: If True, fetch year-by-year splits

    Returns:
        If all_years is False: Dict with 'left' and 'right' keys
        If all_years is True: List of dicts, one per year, each with 'year', 'left', 'right'
    """
    try:
        url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"

        group = 'pitching' if player_type == 'pitcher' else 'hitting'
        params = {
            'stats': 'statSplits',
            'group': group,
            'sitCodes': 'vr,vl'  # vs Right, vs Left
        }

        if year and not all_years:
            params['season'] = year

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()

        if 'stats' not in data or len(data['stats']) == 0:
            return None

        splits = data['stats'][0].get('splits', [])

        if not splits:
            return None

        if all_years:
            # Fetch splits for each year (2022-2025)
            years_data = {}
            for yr in range(2022, 2026):
                try:
                    year_response = requests.get(url, params={**params, 'season': yr}, timeout=10)
                    year_response.raise_for_status()
                    year_data = year_response.json()

                    if 'stats' not in year_data or len(year_data['stats']) == 0:
                        continue

                    year_splits = year_data['stats'][0].get('splits', [])
                    if not year_splits:
                        continue

                    years_data[yr] = {}

                    for split in year_splits:
                        split_code = split.get('split', {}).get('code')
                        stat = split.get('stat', {})

                        # Parse batting average stats
                        def parse_avg_stat(value):
                            if not value or value == '-.--':
                                return 0.0
                            try:
                                if value.startswith('.'):
                                    return float('0' + value)
                                return float(value)
                            except (ValueError, TypeError):
                                return 0.0

                        if player_type == 'pitcher':
                            split_data = {
                                'pa': stat.get('battersFaced', 0),
                                'ab': stat.get('atBats', 0),
                                'h': stat.get('hits', 0),
                                'doubles': stat.get('doubles', 0),
                                'triples': stat.get('triples', 0),
                                'hr': stat.get('homeRuns', 0),
                                'bb': stat.get('baseOnBalls', 0),
                                'so': stat.get('strikeOuts', 0),
                                'ba': parse_avg_stat(stat.get('avg')),
                                'obp': parse_avg_stat(stat.get('obp')),
                                'slg': parse_avg_stat(stat.get('slg')),
                                'ops': parse_avg_stat(stat.get('ops')),
                                'ip': stat.get('inningsPitched', '0'),
                                'whip': parse_avg_stat(stat.get('whip')),
                                'era': parse_avg_stat(stat.get('earnedRunAverage')),
                                'k9': parse_avg_stat(stat.get('strikeoutsPer9Inn')),
                                'bb9': parse_avg_stat(stat.get('walksPer9Inn')),
                            }
                        else:
                            split_data = {
                                'pa': stat.get('plateAppearances', 0),
                                'ab': stat.get('atBats', 0),
                                'h': stat.get('hits', 0),
                                'doubles': stat.get('doubles', 0),
                                'triples': stat.get('triples', 0),
                                'hr': stat.get('homeRuns', 0),
                                'rbi': stat.get('rbi', 0),
                                'bb': stat.get('baseOnBalls', 0),
                                'so': stat.get('strikeOuts', 0),
                                'ba': parse_avg_stat(stat.get('avg')),
                                'obp': parse_avg_stat(stat.get('obp')),
                                'slg': parse_avg_stat(stat.get('slg')),
                                'ops': parse_avg_stat(stat.get('ops')),
                            }

                        if split_code == 'vl':
                            years_data[yr]['left'] = split_data
                        elif split_code == 'vr':
                            years_data[yr]['right'] = split_data

                except Exception:
                    # Skip years with errors
                    continue

            # Convert to list format
            result = []
            for yr in sorted(years_data.keys()):
                if 'left' in years_data[yr] and 'right' in years_data[yr]:
                    result.append({
                        'year': yr,
                        'left': years_data[yr]['left'],
                        'right': years_data[yr]['right']
                    })

            return result if result else None

        result = {}

        for split in splits:
            split_code = split.get('split', {}).get('code')
            stat = split.get('stat', {})

            # Parse batting average stats
            def parse_avg_stat(value):
                if not value or value == '-.--':
                    return 0.0
                try:
                    if value.startswith('.'):
                        return float('0' + value)
                    return float(value)
                except (ValueError, TypeError):
                    return 0.0

            if player_type == 'pitcher':
                # For pitchers: what batters hit against them
                split_data = {
                    'pa': stat.get('battersFaced', 0),
                    'ab': stat.get('atBats', 0),
                    'h': stat.get('hits', 0),
                    'doubles': stat.get('doubles', 0),
                    'triples': stat.get('triples', 0),
                    'hr': stat.get('homeRuns', 0),
                    'bb': stat.get('baseOnBalls', 0),
                    'so': stat.get('strikeOuts', 0),
                    'ba': parse_avg_stat(stat.get('avg')),
                    'obp': parse_avg_stat(stat.get('obp')),
                    'slg': parse_avg_stat(stat.get('slg')),
                    'ops': parse_avg_stat(stat.get('ops')),
                    'ip': stat.get('inningsPitched', '0'),
                    'whip': parse_avg_stat(stat.get('whip')),
                    'era': parse_avg_stat(stat.get('earnedRunAverage')),
                    'k9': parse_avg_stat(stat.get('strikeoutsPer9Inn')),
                    'bb9': parse_avg_stat(stat.get('walksPer9Inn')),
                }
            else:
                # For hitters: their performance
                split_data = {
                    'pa': stat.get('plateAppearances', 0),
                    'ab': stat.get('atBats', 0),
                    'h': stat.get('hits', 0),
                    'doubles': stat.get('doubles', 0),
                    'triples': stat.get('triples', 0),
                    'hr': stat.get('homeRuns', 0),
                    'rbi': stat.get('rbi', 0),
                    'bb': stat.get('baseOnBalls', 0),
                    'so': stat.get('strikeOuts', 0),
                    'ba': parse_avg_stat(stat.get('avg')),
                    'obp': parse_avg_stat(stat.get('obp')),
                    'slg': parse_avg_stat(stat.get('slg')),
                    'ops': parse_avg_stat(stat.get('ops')),
                }

            if split_code == 'vl':
                result['left'] = split_data
            elif split_code == 'vr':
                result['right'] = split_data

        return result if result else None

    except requests.exceptions.RequestException as e:
        print(f"Error fetching platoon splits from API: {e}")
        return None
    except Exception as e:
        print(f"Error processing platoon splits: {e}")
        return None

def cache_platoon_splits(cursor, player_name, player_id, player_type, splits_data, year=None, all_years=False):
    """Cache platoon splits in database

    Args:
        cursor: Database cursor
        player_name: Full player name
        player_id: MLB player ID
        player_type: 'pitcher' or 'hitter'
        splits_data: Dict with 'left' and 'right' keys, or list of year dicts
        year: Optional specific year
        all_years: If True, splits_data is a list of year dicts
    """
    try:
        if all_years:
            # Cache multiple years
            for year_data in splits_data:
                yr = str(year_data['year'])

                # Cache left split
                left_stat = year_data['left']
                cursor.execute('''
                    INSERT OR REPLACE INTO platoon_splits (
                        player_name, player_mlb_id, player_type, year, split_type,
                        pa, ab, h, doubles, triples, hr, rbi, bb, so,
                        ba, obp, slg, ops, ip, whip, era, k9, bb9, last_updated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    player_name, player_id, player_type, yr, 'left',
                    left_stat.get('pa', 0), left_stat.get('ab', 0), left_stat.get('h', 0),
                    left_stat.get('doubles', 0), left_stat.get('triples', 0), left_stat.get('hr', 0),
                    left_stat.get('rbi', 0), left_stat.get('bb', 0), left_stat.get('so', 0),
                    left_stat.get('ba', 0.0), left_stat.get('obp', 0.0), left_stat.get('slg', 0.0),
                    left_stat.get('ops', 0.0), left_stat.get('ip', '0'), left_stat.get('whip', 0.0),
                    left_stat.get('era', 0.0), left_stat.get('k9', 0.0), left_stat.get('bb9', 0.0)
                ))

                # Cache right split
                right_stat = year_data['right']
                cursor.execute('''
                    INSERT OR REPLACE INTO platoon_splits (
                        player_name, player_mlb_id, player_type, year, split_type,
                        pa, ab, h, doubles, triples, hr, rbi, bb, so,
                        ba, obp, slg, ops, ip, whip, era, k9, bb9, last_updated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    player_name, player_id, player_type, yr, 'right',
                    right_stat.get('pa', 0), right_stat.get('ab', 0), right_stat.get('h', 0),
                    right_stat.get('doubles', 0), right_stat.get('triples', 0), right_stat.get('hr', 0),
                    right_stat.get('rbi', 0), right_stat.get('bb', 0), right_stat.get('so', 0),
                    right_stat.get('ba', 0.0), right_stat.get('obp', 0.0), right_stat.get('slg', 0.0),
                    right_stat.get('ops', 0.0), right_stat.get('ip', '0'), right_stat.get('whip', 0.0),
                    right_stat.get('era', 0.0), right_stat.get('k9', 0.0), right_stat.get('bb9', 0.0)
                ))
        else:
            # Cache single year or career
            yr = str(year) if year else 'career'

            # Cache left split
            left_stat = splits_data['left']
            cursor.execute('''
                INSERT OR REPLACE INTO platoon_splits (
                    player_name, player_mlb_id, player_type, year, split_type,
                    pa, ab, h, doubles, triples, hr, rbi, bb, so,
                    ba, obp, slg, ops, ip, whip, era, k9, bb9, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                player_name, player_id, player_type, yr, 'left',
                left_stat.get('pa', 0), left_stat.get('ab', 0), left_stat.get('h', 0),
                left_stat.get('doubles', 0), left_stat.get('triples', 0), left_stat.get('hr', 0),
                left_stat.get('rbi', 0), left_stat.get('bb', 0), left_stat.get('so', 0),
                left_stat.get('ba', 0.0), left_stat.get('obp', 0.0), left_stat.get('slg', 0.0),
                left_stat.get('ops', 0.0), left_stat.get('ip', '0'), left_stat.get('whip', 0.0),
                left_stat.get('era', 0.0), left_stat.get('k9', 0.0), left_stat.get('bb9', 0.0)
            ))

            # Cache right split
            right_stat = splits_data['right']
            cursor.execute('''
                INSERT OR REPLACE INTO platoon_splits (
                    player_name, player_mlb_id, player_type, year, split_type,
                    pa, ab, h, doubles, triples, hr, rbi, bb, so,
                    ba, obp, slg, ops, ip, whip, era, k9, bb9, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                player_name, player_id, player_type, yr, 'right',
                right_stat.get('pa', 0), right_stat.get('ab', 0), right_stat.get('h', 0),
                right_stat.get('doubles', 0), right_stat.get('triples', 0), right_stat.get('hr', 0),
                right_stat.get('rbi', 0), right_stat.get('bb', 0), right_stat.get('so', 0),
                right_stat.get('ba', 0.0), right_stat.get('obp', 0.0), right_stat.get('slg', 0.0),
                right_stat.get('ops', 0.0), right_stat.get('ip', '0'), right_stat.get('whip', 0.0),
                right_stat.get('era', 0.0), right_stat.get('k9', 0.0), right_stat.get('bb9', 0.0)
            ))

        cursor.connection.commit()
    except Exception as e:
        print(f"Error caching platoon splits: {e}")
        cursor.connection.rollback()

def get_cached_platoon_splits(cursor, player_name, year=None, all_years=False):
    """Get cached platoon splits from database

    Returns:
        If all_years: List of year dicts with 'year', 'left', 'right'
        Otherwise: Dict with 'left' and 'right' keys
        None if not cached
    """
    try:
        if all_years:
            # Get all years (2022-2025)
            years_data = []
            for yr in range(2022, 2026):
                cursor.execute('''
                    SELECT split_type, pa, ab, h, doubles, triples, hr, rbi, bb, so,
                           ba, obp, slg, ops, ip, whip, era, k9, bb9
                    FROM platoon_splits
                    WHERE player_name = ? AND year = ?
                ''', (player_name, str(yr)))

                rows = cursor.fetchall()
                if len(rows) == 2:  # Must have both left and right
                    year_splits = {'year': yr}
                    for row in rows:
                        split_data = {
                            'pa': row[1], 'ab': row[2], 'h': row[3], 'doubles': row[4],
                            'triples': row[5], 'hr': row[6], 'rbi': row[7], 'bb': row[8],
                            'so': row[9], 'ba': row[10], 'obp': row[11], 'slg': row[12],
                            'ops': row[13], 'ip': row[14], 'whip': row[15], 'era': row[16],
                            'k9': row[17], 'bb9': row[18]
                        }
                        if row[0] == 'left':
                            year_splits['left'] = split_data
                        else:
                            year_splits['right'] = split_data

                    if 'left' in year_splits and 'right' in year_splits:
                        years_data.append(year_splits)

            return years_data if years_data else None
        else:
            # Get single year or career
            yr = str(year) if year else 'career'
            cursor.execute('''
                SELECT split_type, pa, ab, h, doubles, triples, hr, rbi, bb, so,
                       ba, obp, slg, ops, ip, whip, era, k9, bb9
                FROM platoon_splits
                WHERE player_name = ? AND year = ?
            ''', (player_name, yr))

            rows = cursor.fetchall()
            if len(rows) != 2:
                return None

            result = {}
            for row in rows:
                split_data = {
                    'pa': row[1], 'ab': row[2], 'h': row[3], 'doubles': row[4],
                    'triples': row[5], 'hr': row[6], 'rbi': row[7], 'bb': row[8],
                    'so': row[9], 'ba': row[10], 'obp': row[11], 'slg': row[12],
                    'ops': row[13], 'ip': row[14], 'whip': row[15], 'era': row[16],
                    'k9': row[17], 'bb9': row[18]
                }
                if row[0] == 'left':
                    result['left'] = split_data
                else:
                    result['right'] = split_data

            return result if 'left' in result and 'right' in result else None
    except Exception as e:
        print(f"Error retrieving cached platoon splits: {e}")
        return None
