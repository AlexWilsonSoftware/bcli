#!/usr/bin/env python3
import click
import sqlite3
import os
import re
import mlb_api
import unicodedata

DB_PATH = os.path.join(os.path.dirname(__file__), 'baseball_stats.db')

def get_db_connection():
    return sqlite3.connect(DB_PATH)

def remove_accents(text):
    """Remove accents from unicode string"""
    nfd = unicodedata.normalize('NFD', text)
    return ''.join(char for char in nfd if unicodedata.category(char) != 'Mn')

def parse_name_query(name_query):
    if '.' not in name_query:
        return None, name_query

    parts = name_query.split('.', 1)
    first_pattern = parts[0]
    last_pattern = parts[1]

    return first_pattern, last_pattern

def find_player(cursor, name_query):
    first_pattern, last_pattern = parse_name_query(name_query)

    # Remove accents from search patterns
    if first_pattern is None:
        search_pattern = remove_accents(last_pattern.lower())
        first_search = None
        last_search = search_pattern
    else:
        first_search = remove_accents(first_pattern.lower())
        last_search = remove_accents(last_pattern.lower())
        search_pattern = f'{first_search}{last_search}'

    # First try exact match with original query
    if first_pattern is None:
        pattern = f'%{last_pattern.lower()}%'
    else:
        pattern = f'{first_pattern.lower()}%{last_pattern.lower()}%'

    cursor.execute('''
        SELECT * FROM pitcher_stats
        WHERE LOWER(player) LIKE ? AND ip >= 5
        ORDER BY year ASC, team
    ''', (pattern,))
    pitcher_matches = cursor.fetchall()

    cursor.execute('''
        SELECT * FROM hitter_stats
        WHERE LOWER(player) LIKE ? AND ab >= 5
        ORDER BY year ASC, team
    ''', (pattern,))
    hitter_matches = cursor.fetchall()

    # If no exact matches, try accent-insensitive matching
    if not pitcher_matches and not hitter_matches:
        cursor.execute('SELECT * FROM pitcher_stats WHERE ip >= 5 ORDER BY year ASC, team')
        all_pitchers = cursor.fetchall()

        cursor.execute(f"SELECT * FROM pitcher_stats LIMIT 1")
        column_names = [desc[0] for desc in cursor.description]
        player_col_idx = column_names.index('player')

        pitcher_matches = []
        for row in all_pitchers:
            normalized_name = remove_accents(row[player_col_idx].lower())
            if first_search is None:
                if last_search in normalized_name:
                    pitcher_matches.append(row)
            else:
                # For dot notation: check if starts with first pattern and contains last pattern
                if normalized_name.startswith(first_search) and last_search in normalized_name:
                    pitcher_matches.append(row)

        cursor.execute('SELECT * FROM hitter_stats WHERE ab >= 5 ORDER BY year ASC, team')
        all_hitters = cursor.fetchall()

        cursor.execute(f"SELECT * FROM hitter_stats LIMIT 1")
        column_names = [desc[0] for desc in cursor.description]
        player_col_idx = column_names.index('player')

        hitter_matches = []
        for row in all_hitters:
            normalized_name = remove_accents(row[player_col_idx].lower())
            if first_search is None:
                if last_search in normalized_name:
                    hitter_matches.append(row)
            else:
                # For dot notation: check if starts with first pattern and contains last pattern
                if normalized_name.startswith(first_search) and last_search in normalized_name:
                    hitter_matches.append(row)

    return pitcher_matches, hitter_matches

def fuzzy_find_player(cursor, name_query):
    """Find players using accent-insensitive fuzzy matching"""
    # Remove accents from search query
    normalized_query = remove_accents(name_query.lower())

    # Get all unique player names
    cursor.execute('SELECT DISTINCT player FROM pitcher_stats WHERE ip >= 5')
    pitchers = [row[0] for row in cursor.fetchall()]

    cursor.execute('SELECT DISTINCT player FROM hitter_stats WHERE ab >= 5')
    hitters = [row[0] for row in cursor.fetchall()]

    # Find matches by comparing accent-removed names
    fuzzy_pitcher_matches = []
    for pitcher in pitchers:
        normalized_pitcher = remove_accents(pitcher.lower())
        if normalized_query in normalized_pitcher:
            fuzzy_pitcher_matches.append(pitcher)

    fuzzy_hitter_matches = []
    for hitter in hitters:
        normalized_hitter = remove_accents(hitter.lower())
        if normalized_query in normalized_hitter:
            fuzzy_hitter_matches.append(hitter)

    return fuzzy_pitcher_matches, fuzzy_hitter_matches

def format_stat_value(value):
    if value is None:
        return 'N/A'
    if isinstance(value, (int, float)):
        return str(value)
    return value

def parse_positions(pos_str):
    """Convert position codes to readable format

    Examples:
        *6/D -> *SS / DH
        *D1/H97 -> *DH, P / PH, RF, LF
        *98/HD4 -> *RF, CF / PH, DH, 2B
    """
    if not pos_str:
        return ''

    # Position mapping
    pos_map = {
        '1': 'P',
        '2': 'C',
        '3': '1B',
        '4': '2B',
        '5': '3B',
        '6': 'SS',
        '7': 'LF',
        '8': 'CF',
        '9': 'RF',
        'D': 'DH',
        'H': 'PH'
    }

    # Check if starts with *
    has_star = pos_str.startswith('*')
    if has_star:
        pos_str = pos_str[1:]

    # Split by /
    parts = pos_str.split('/')
    result_parts = []

    for part in parts:
        # Parse each character as a position in this part
        positions_in_part = []
        seen_in_part = set()

        for char in part:
            if char in pos_map:
                pos_name = pos_map[char]
                if pos_name not in seen_in_part:
                    seen_in_part.add(pos_name)
                    positions_in_part.append(pos_name)

        if positions_in_part:
            result_parts.append(', '.join(positions_in_part))

    result = ' / '.join(result_parts)
    if has_star:
        result = '*' + result

    return result

def parse_awards(awards_str):
    if not awards_str:
        return None

    awards = []
    remaining = awards_str

    patterns = [
        (r'MVP-(\d{1,2})', lambda m: f'MVP-{m.group(1)}'),
        (r'CYA-(\d{1,2})', lambda m: f'CYA-{m.group(1)}'),
        (r'ROY-(\d{1,2})', lambda m: f'ROY-{m.group(1)}'),
        (r'AS', lambda m: 'AS'),
        (r'GG', lambda m: 'GG'),
        (r'SS', lambda m: 'SS'),
    ]

    pos = 0
    while pos < len(remaining):
        matched = False
        for pattern, formatter in patterns:
            match = re.match(pattern, remaining[pos:])
            if match:
                awards.append(formatter(match))
                pos += len(match.group(0))
                matched = True
                break
        if not matched:
            pos += 1

    return ', '.join(awards) if awards else awards_str

def get_column_index(cursor, column_name):
    column_names = [desc[0] for desc in cursor.description]
    try:
        return column_names.index(column_name)
    except ValueError:
        return None

def get_stat_category(stat_key):
    """Returns 'pitcher', 'hitter', or 'common' for a given stat key"""
    pitcher_only_stats = {
        'w', 'l', 'w_l_pct', 'era', 'gs', 'gf', 'cg', 'sho', 'sv', 'ip',
        'er', 'ibb', 'hbp', 'bk', 'wp', 'bf', 'era_plus', 'fip', 'whip',
        'h9', 'hr9', 'bb9', 'so9', 'so_bb'
    }

    hitter_only_stats = {
        'pa', 'ab', 'doubles', 'triples', 'rbi', 'sb', 'cs', 'ba', 'obp',
        'slg', 'ops', 'ops_plus', 'roba', 'rbat_plus', 'tb', 'gidp',
        'sh', 'sf', 'pos'
    }

    # Common stats: year, age, team, lg, war, g, h, r, hr, bb, so, awards

    if stat_key in pitcher_only_stats:
        return 'pitcher'
    elif stat_key in hitter_only_stats:
        return 'hitter'
    else:
        return 'common'

def get_full_team_name(abbr):
    """Convert team abbreviation to full name used in team stats"""
    team_mapping = {
        'ARI': 'Arizona Diamondbacks',
        'ATH': 'Athletics',
        'ATL': 'Atlanta Braves',
        'BAL': 'Baltimore Orioles',
        'BOS': 'Boston Red Sox',
        'CHC': 'Chicago Cubs',
        'CHW': 'Chicago White Sox',
        'CIN': 'Cincinnati Reds',
        'CLE': 'Cleveland Guardians',
        'COL': 'Colorado Rockies',
        'DET': 'Detroit Tigers',
        'HOU': 'Houston Astros',
        'KCR': 'Kansas City Royals',
        'LAA': 'Los Angeles Angels',
        'LAD': 'Los Angeles Dodgers',
        'MIA': 'Miami Marlins',
        'MIL': 'Milwaukee Brewers',
        'MIN': 'Minnesota Twins',
        'NYM': 'New York Mets',
        'NYY': 'New York Yankees',
        'OAK': 'Oakland Athletics',
        'PHI': 'Philadelphia Phillies',
        'PIT': 'Pittsburgh Pirates',
        'SDP': 'San Diego Padres',
        'SEA': 'Seattle Mariners',
        'SFG': 'San Francisco Giants',
        'STL': 'St. Louis Cardinals',
        'TBR': 'Tampa Bay Rays',
        'TEX': 'Texas Rangers',
        'TOR': 'Toronto Blue Jays',
        'WSN': 'Washington Nationals'
    }
    return team_mapping.get(abbr, abbr)

def list_matching_players(cursor, matches, player_type):
    """List all unique matching players without showing full stats"""
    cursor.execute(f"SELECT * FROM {player_type}_stats LIMIT 1")
    column_names = [desc[0] for desc in cursor.description]
    player_col_idx = column_names.index('player')
    team_col_idx = column_names.index('team')
    year_col_idx = column_names.index('year')

    unique_players = set(match[player_col_idx] for match in matches)

    for player in sorted(unique_players):
        player_matches = [match for match in matches if match[player_col_idx] == player]

        season_2025 = [m for m in player_matches if m[year_col_idx] == 2025]

        if season_2025:
            teams = [m[team_col_idx] for m in season_2025 if '2TM' not in m[team_col_idx] and '3TM' not in m[team_col_idx]]
            if not teams:
                teams = [season_2025[0][team_col_idx]]
        else:
            most_recent_year = max(m[year_col_idx] for m in player_matches)
            recent_matches = [m for m in player_matches if m[year_col_idx] == most_recent_year]
            teams = [m[team_col_idx] for m in recent_matches if '2TM' not in m[team_col_idx] and '3TM' not in m[team_col_idx]]
            if not teams:
                teams = [recent_matches[0][team_col_idx]]

        if len(teams) > 1:
            click.echo(f"  - {player} ({', '.join(teams)})")
        else:
            click.echo(f"  - {player} ({teams[0]})")

def render_player(cursor, matches, player_type, stats, year, comparison_mode=None):
    cursor.execute(f"SELECT * FROM {player_type}_stats LIMIT 1")
    column_names = [desc[0] for desc in cursor.description]

    if year:
        if len(year) == 2 and year.isdigit():
            year_filter = int(f"20{year}")
        elif len(year) == 4 and year.isdigit():
            year_filter = int(year)
        else:
            click.echo(f"Error: Invalid year format '{year}'. Use 2022 or 22.")
            return

        year_col_idx = get_column_index(cursor, 'year')
        matches = [m for m in matches if m[year_col_idx] == year_filter]

        if not matches:
            click.echo(f"No {player_type} data for {year_filter}")
            return

    # Check for multiple players BEFORE printing header
    if len(matches) > 1:
        player_col_idx = get_column_index(cursor, 'player')
        unique_players = set(match[player_col_idx] for match in matches)

        if len(unique_players) > 1:
            team_col_idx = get_column_index(cursor, 'team')
            year_col_idx = get_column_index(cursor, 'year')

            click.echo(f"Error: Multiple {player_type}s found matching:")

            for player in sorted(unique_players):
                player_matches = [match for match in matches if match[player_col_idx] == player]

                season_2025 = [m for m in player_matches if m[year_col_idx] == 2025]

                if season_2025:
                    teams = [m[team_col_idx] for m in season_2025 if '2TM' not in m[team_col_idx] and '3TM' not in m[team_col_idx]]
                    if not teams:
                        teams = [season_2025[0][team_col_idx]]
                else:
                    most_recent_year = max(m[year_col_idx] for m in player_matches)
                    recent_matches = [m for m in player_matches if m[year_col_idx] == most_recent_year]
                    teams = [m[team_col_idx] for m in recent_matches if '2TM' not in m[team_col_idx] and '3TM' not in m[team_col_idx]]
                    if not teams:
                        teams = [recent_matches[0][team_col_idx]]

                if len(teams) > 1:
                    click.echo(f"  - {player} ({', '.join(teams)})")
                else:
                    click.echo(f"  - {player} ({teams[0]})")
            return

    # Now we know there's only one player, print the header
    first_player = dict(zip(column_names, matches[0]))
    player_name = first_player['player']
    header_text = f"{player_name} ({player_type.upper()})"
    click.echo(f"\n{header_text}")

    if stats:
        # Build custom stats list starting with base columns
        all_stats = [('Season', 'year'), ('Age', 'age'), ('Team', 'team'), ('Lg', 'lg')]

        # Track invalid stats for this player type
        invalid_stats = []

        # Add requested stats
        for stat in stats:
            stat_lower = stat.lower()
            if stat_lower == 'w-l%':
                stat_key = 'w_l_pct'
                stat_label = 'W-L%'
            elif stat_lower == 'so/bb':
                stat_key = 'so_bb'
                stat_label = 'SO/BB'
            elif stat_lower == 'era+':
                stat_key = 'era_plus'
                stat_label = 'ERA+'
            elif stat_lower == 'ops+':
                stat_key = 'ops_plus'
                stat_label = 'OPS+'
            elif stat_lower == 'rbat+':
                stat_key = 'rbat_plus'
                stat_label = 'Rbat+'
            elif stat_lower == '2b':
                stat_key = 'doubles'
                stat_label = '2B'
            elif stat_lower == '3b':
                stat_key = 'triples'
                stat_label = '3B'
            elif stat_lower == 'h/9':
                stat_key = 'h9'
                stat_label = 'H/9'
            elif stat_lower == 'hr/9':
                stat_key = 'hr9'
                stat_label = 'HR/9'
            elif stat_lower == 'bb/9':
                stat_key = 'bb9'
                stat_label = 'BB/9'
            elif stat_lower == 'so/9':
                stat_key = 'so9'
                stat_label = 'SO/9'
            else:
                stat_key = stat_lower.replace('-', '_').replace('/', '_')
                stat_label = stat.upper()

            # Check if this stat exists in the database
            if stat_key not in column_names:
                invalid_stats.append((stat_label, player_type))
                continue

            # Check if this stat is valid for the player type
            stat_category = get_stat_category(stat_key)
            if stat_category != 'common' and stat_category != player_type:
                invalid_stats.append((stat_label, player_type))
                continue

            all_stats.append((stat_label, stat_key))

        # If all requested stats are invalid for this player type, show message and return
        if len(all_stats) == 4:  # Only has base columns (year, age, team, lg)
            if invalid_stats:
                stats_list = ', '.join([s[0] for s in invalid_stats])
                click.echo(f"Stats not available for {player_type}s: {stats_list}")
            return
    else:
        if player_type == 'pitcher':
            all_stats = [
                ('Season', 'year'), ('Age', 'age'), ('Team', 'team'), ('Lg', 'lg'),
                ('WAR', 'war'), ('W', 'w'), ('L', 'l'), ('W-L%', 'w_l_pct'), ('ERA', 'era'),
                ('G', 'g'), ('GS', 'gs'), ('GF', 'gf'), ('CG', 'cg'), ('SHO', 'sho'), ('SV', 'sv'),
                ('IP', 'ip'), ('H', 'h'), ('R', 'r'), ('ER', 'er'), ('HR', 'hr'),
                ('BB', 'bb'), ('IBB', 'ibb'), ('SO', 'so'), ('HBP', 'hbp'), ('BK', 'bk'), ('WP', 'wp'), ('BF', 'bf'),
                ('ERA+', 'era_plus'), ('FIP', 'fip'), ('WHIP', 'whip'),
                ('H/9', 'h9'), ('HR/9', 'hr9'), ('BB/9', 'bb9'), ('SO/9', 'so9'), ('SO/BB', 'so_bb'),
                ('Awards', 'awards'),
            ]
        else:
            all_stats = [
                ('Season', 'year'), ('Age', 'age'), ('Team', 'team'), ('Lg', 'lg'),
                ('WAR', 'war'), ('G', 'g'), ('PA', 'pa'), ('AB', 'ab'), ('R', 'r'), ('H', 'h'),
                ('2B', 'doubles'), ('3B', 'triples'), ('HR', 'hr'), ('RBI', 'rbi'),
                ('SB', 'sb'), ('CS', 'cs'), ('BB', 'bb'), ('SO', 'so'),
                ('BA', 'ba'), ('OBP', 'obp'), ('SLG', 'slg'), ('OPS', 'ops'),
                ('OPS+', 'ops_plus'), ('rOBA', 'roba'), ('Rbat+', 'rbat_plus'),
                ('TB', 'tb'), ('GIDP', 'gidp'), ('HBP', 'hbp'), ('SH', 'sh'), ('SF', 'sf'), ('IBB', 'ibb'),
                ('Pos', 'pos'), ('Awards', 'awards'),
            ]

    # If comparison mode, filter stats early and fetch averages
    comparison_averages = {}
    if comparison_mode:
        stat_mapping_hitter = {
            'g': 'g', 'pa': 'pa', 'ab': 'ab', 'r': 'r', 'h': 'h',
            'doubles': 'doubles', 'triples': 'triples', 'hr': 'hr', 'rbi': 'rbi',
            'sb': 'sb', 'cs': 'cs', 'bb': 'bb', 'so': 'so',
            'ba': 'ba', 'obp': 'obp', 'slg': 'slg', 'ops': 'ops',
            'ops_plus': 'ops_plus', 'tb': 'tb', 'gidp': 'gdp',
            'hbp': 'hbp', 'sh': 'sh', 'sf': 'sf', 'ibb': 'ibb'
        }
        stat_mapping_pitcher = {
            'w': 'w', 'l': 'l', 'w_l_pct': 'w_l_pct', 'era': 'era',
            'g': 'g', 'gs': 'gs', 'gf': 'gf', 'cg': 'cg',
            'sho': 'c_sho', 'sv': 'sv', 'ip': 'ip', 'h': 'h',
            'r': 'r', 'er': 'er', 'hr': 'hr', 'bb': 'bb',
            'ibb': 'ibb', 'so': 'so', 'hbp': 'hbp', 'bk': 'bk',
            'wp': 'wp', 'bf': 'bf', 'era_plus': 'era_plus',
            'fip': 'fip', 'whip': 'whip', 'h9': 'h9', 'hr9': 'hr9',
            'bb9': 'bb9', 'so9': 'so9', 'so_bb': 'so_w'
        }
        stat_mapping = stat_mapping_pitcher if player_type == 'pitcher' else stat_mapping_hitter

        team_table = 'team_pitcher_stats' if player_type == 'pitcher' else 'team_hitter_stats'
        years = set(dict(zip(column_names, m)).get('year') for m in matches)

        for yr in years:
            if comparison_mode == 'league':
                cursor.execute(f"SELECT * FROM {team_table} WHERE year = ? AND tm = ?", (yr, 'League Average'))
            else:  # team mode
                year_matches = [m for m in matches if dict(zip(column_names, m)).get('year') == yr]
                if year_matches:
                    player_team = dict(zip(column_names, year_matches[0])).get('team')
                    if player_team and '2TM' not in player_team and '3TM' not in player_team:
                        team_full_name = get_full_team_name(player_team)
                        cursor.execute(f"SELECT * FROM {team_table} WHERE year = ? AND tm = ?", (yr, team_full_name))
                    else:
                        continue

            avg_row = cursor.fetchone()
            if avg_row:
                avg_column_names = [desc[0] for desc in cursor.description]
                comparison_averages[yr] = dict(zip(avg_column_names, avg_row))

        # Filter all_stats to only show comparable stats
        if comparison_averages:
            sample_avg = next(iter(comparison_averages.values()))

            # Define which stats to show for comparison mode
            if player_type == 'pitcher':
                comparison_stats = {'era', 'w_l_pct', 'whip', 'fip', 'era_plus', 'h9', 'hr9', 'bb9', 'so9', 'so_bb'}
            else:  # hitter
                # roba and rbat_plus not available in team stats
                comparison_stats = {'ba', 'obp', 'slg', 'ops', 'ops_plus'}

            filtered_stats = []
            for label, key in all_stats:
                if key in ['year', 'age', 'team', 'lg', 'awards']:
                    filtered_stats.append((label, key))
                # Only include if in comparison_stats, has mapping, and exists in avg data
                elif key in comparison_stats and key in stat_mapping and stat_mapping[key] in sample_avg:
                    filtered_stats.append((label, key))
            all_stats = filtered_stats

    # Table rendering logic (shared by both filtered and non-filtered stats)
    season_2025_rows = []
    season_2025_data = []
    historical_rows = []
    historical_data = []

    prev_year = None
    gap_rows = []

    for idx, player_data in enumerate(matches):
        player_dict = dict(zip(column_names, player_data))
        current_year = player_dict.get('year')

        row_values = []
        for label, key in all_stats:
            if key == 'awards':
                awards_val = player_dict.get('awards', '')
                if awards_val:
                    parsed = parse_awards(awards_val)
                    row_values.append(parsed if parsed else '')
                else:
                    row_values.append('')
            elif key == 'pos':
                pos_val = player_dict.get('pos', '')
                if pos_val:
                    parsed = parse_positions(pos_val)
                    row_values.append(parsed if parsed else '')
                else:
                    row_values.append('')
            elif key in player_dict and player_dict[key] is not None:
                row_values.append(format_stat_value(player_dict[key]))
            else:
                row_values.append('')

        if current_year == 2025:
            season_2025_rows.append(row_values)
            season_2025_data.append(player_dict)
        else:
            if len(historical_rows) > 0 and prev_year is not None and current_year is not None:
                year_gap = current_year - prev_year
                if year_gap > 1:
                    missing_years = list(range(prev_year + 1, current_year))
                    gap_msg = f"[Did not play in {', '.join(map(str, missing_years))}]"
                    gap_rows.append((len(historical_rows), gap_msg))

            historical_rows.append(row_values)
            historical_data.append(player_dict)
            prev_year = current_year

    all_rows = season_2025_rows + historical_rows
    all_row_data = season_2025_data + historical_data

    traded_allstar_years = set()
    traded_years = set()
    for row_data in all_row_data:
        team = row_data.get('team', '')
        year = row_data.get('year')
        awards = row_data.get('awards', '')
        if '2TM' in team or '3TM' in team:
            traded_years.add(year)
            if awards and 'AS' in awards:
                traded_allstar_years.add(year)

    col_widths = []
    for col_idx, (label, key) in enumerate(all_stats):
        max_width = len(label)
        for row in all_rows:
            max_width = max(max_width, len(row[col_idx]))

        # In comparison mode, account for the average row labels in the year column
        if comparison_mode and key == 'year' and comparison_averages:
            comparison_label = "League Avg" if comparison_mode == 'league' else "Team Avg"
            for yr in comparison_averages.keys():
                avg_label_len = len(f"{yr} {comparison_label}")
                max_width = max(max_width, avg_label_len)

        # In comparison mode, account for the average values in stat columns
        if comparison_mode and comparison_averages and key not in ['year', 'age', 'team', 'lg', 'awards']:
            stat_mapping_hitter = {
                'g': 'g', 'pa': 'pa', 'ab': 'ab', 'r': 'r', 'h': 'h',
                'doubles': 'doubles', 'triples': 'triples', 'hr': 'hr', 'rbi': 'rbi',
                'sb': 'sb', 'cs': 'cs', 'bb': 'bb', 'so': 'so',
                'ba': 'ba', 'obp': 'obp', 'slg': 'slg', 'ops': 'ops',
                'ops_plus': 'ops_plus', 'tb': 'tb', 'gidp': 'gdp',
                'hbp': 'hbp', 'sh': 'sh', 'sf': 'sf', 'ibb': 'ibb'
            }
            stat_mapping_pitcher = {
                'w': 'w', 'l': 'l', 'w_l_pct': 'w_l_pct', 'era': 'era',
                'g': 'g', 'gs': 'gs', 'gf': 'gf', 'cg': 'cg',
                'sho': 'c_sho', 'sv': 'sv', 'ip': 'ip', 'h': 'h',
                'r': 'r', 'er': 'er', 'hr': 'hr', 'bb': 'bb',
                'ibb': 'ibb', 'so': 'so', 'hbp': 'hbp', 'bk': 'bk',
                'wp': 'wp', 'bf': 'bf', 'era_plus': 'era_plus',
                'fip': 'fip', 'whip': 'whip', 'h9': 'h9', 'hr9': 'hr9',
                'bb9': 'bb9', 'so9': 'so9', 'so_bb': 'so_w'
            }
            mapping = stat_mapping_pitcher if player_type == 'pitcher' else stat_mapping_hitter
            avg_key = mapping.get(key)
            if avg_key:
                for yr, avg_dict in comparison_averages.items():
                    if avg_key in avg_dict:
                        val = avg_dict[avg_key]
                        val_str = format_stat_value(val) if val is not None else ''
                        max_width = max(max_width, len(val_str))

        col_widths.append(max_width)

    def calculate_yearly_league_leaders():
        leaders_by_year = {}

        if player_type == 'pitcher':
            lower_is_better = {'era', 'fip', 'whip', 'h9', 'hr9', 'bb9'}
            rate_stats_needing_qualification = {'era', 'whip', 'fip', 'h9', 'hr9', 'bb9', 'so9', 'w_l_pct', 'era_plus', 'so_bb'}
            qual_field = 'ip'
            qual_threshold = 162
            table_name = 'pitcher_stats'
        else:
            lower_is_better = {}
            rate_stats_needing_qualification = {'ba', 'obp', 'slg', 'ops', 'ops_plus', 'roba', 'rbat_plus'}
            qual_field = 'pa'
            qual_threshold = 502
            table_name = 'hitter_stats'

        years = set(row.get('year') for row in all_row_data if row.get('year'))

        for year in years:
            leaders_by_year[year] = {}

            cursor.execute(f'''
                SELECT * FROM {table_name}
                WHERE year = ?
            ''', (year,))
            all_players_year = cursor.fetchall()
            year_data = [dict(zip(column_names, row)) for row in all_players_year]

            for label, key in all_stats:
                if key in ['year', 'age', 'team', 'lg', 'awards', 'pos']:
                    continue

                leaders_by_year[year][key] = {'NL': None, 'AL': None}

                for league in ['NL', 'AL']:
                    league_data = [row for row in year_data if row.get('lg') == league]
                    if not league_data:
                        continue

                    stat_values = []
                    for row in league_data:
                        val = row.get(key)
                        qual_val = row.get(qual_field)

                        if val is not None and val != '':
                            try:
                                if key in rate_stats_needing_qualification:
                                    if qual_val is None:
                                        continue
                                    qual_float = float(qual_val)
                                    if qual_float < qual_threshold:
                                        continue

                                stat_values.append((float(val), row))
                            except (ValueError, TypeError):
                                pass

                    if not stat_values:
                        continue

                    if key in lower_is_better:
                        leaders_by_year[year][key][league] = min(stat_values, key=lambda x: x[0])[1]
                    else:
                        leaders_by_year[year][key][league] = max(stat_values, key=lambda x: x[0])[1]

        return leaders_by_year

    league_leaders = calculate_yearly_league_leaders()

    # Get stat_mapping for use in comparison formatting
    stat_mapping_hitter = {
        'g': 'g', 'pa': 'pa', 'ab': 'ab', 'r': 'r', 'h': 'h',
        'doubles': 'doubles', 'triples': 'triples', 'hr': 'hr', 'rbi': 'rbi',
        'sb': 'sb', 'cs': 'cs', 'bb': 'bb', 'so': 'so',
        'ba': 'ba', 'obp': 'obp', 'slg': 'slg', 'ops': 'ops',
        'ops_plus': 'ops_plus', 'tb': 'tb', 'gidp': 'gdp',
        'hbp': 'hbp', 'sh': 'sh', 'sf': 'sf', 'ibb': 'ibb'
    }
    stat_mapping_pitcher = {
        'w': 'w', 'l': 'l', 'w_l_pct': 'w_l_pct', 'era': 'era',
        'g': 'g', 'gs': 'gs', 'gf': 'gf', 'cg': 'cg',
        'sho': 'c_sho', 'sv': 'sv', 'ip': 'ip', 'h': 'h',
        'r': 'r', 'er': 'er', 'hr': 'hr', 'bb': 'bb',
        'ibb': 'ibb', 'so': 'so', 'hbp': 'hbp', 'bk': 'bk',
        'wp': 'wp', 'bf': 'bf', 'era_plus': 'era_plus',
        'fip': 'fip', 'whip': 'whip', 'h9': 'h9', 'hr9': 'hr9',
        'bb9': 'bb9', 'so9': 'so9', 'so_bb': 'so_w'
    }
    stat_mapping = stat_mapping_pitcher if player_type == 'pitcher' else stat_mapping_hitter

    header_parts = []
    for idx, (label, key) in enumerate(all_stats):
        header_parts.append(label.ljust(col_widths[idx]))
    header_line = "  ".join(header_parts)

    # Print separator line matching header width
    separator_width = max(len(header_text), len(header_line))
    click.echo("=" * separator_width)

    click.echo(header_line)

    def get_stat_formatting(year, player_league, player_data, stat_key):
        # If comparison mode, check if above average
        if comparison_mode and year in comparison_averages:
            avg_dict = comparison_averages[year]

            # Map player stat keys to team stat keys
            stat_mapping_hitter = {
                'g': 'g', 'pa': 'pa', 'ab': 'ab', 'r': 'r', 'h': 'h',
                'doubles': 'doubles', 'triples': 'triples', 'hr': 'hr', 'rbi': 'rbi',
                'sb': 'sb', 'cs': 'cs', 'bb': 'bb', 'so': 'so',
                'ba': 'ba', 'obp': 'obp', 'slg': 'slg', 'ops': 'ops',
                'ops_plus': 'ops_plus', 'tb': 'tb', 'gidp': 'gdp',
                'hbp': 'hbp', 'sh': 'sh', 'sf': 'sf', 'ibb': 'ibb'
            }

            stat_mapping_pitcher = {
                'w': 'w', 'l': 'l', 'w_l_pct': 'w_l_pct', 'era': 'era',
                'g': 'g', 'gs': 'gs', 'gf': 'gf', 'cg': 'cg',
                'sho': 'c_sho', 'sv': 'sv', 'ip': 'ip', 'h': 'h',
                'r': 'r', 'er': 'er', 'hr': 'hr', 'bb': 'bb',
                'ibb': 'ibb', 'so': 'so', 'hbp': 'hbp', 'bk': 'bk',
                'wp': 'wp', 'bf': 'bf', 'era_plus': 'era_plus',
                'fip': 'fip', 'whip': 'whip', 'h9': 'h9', 'hr9': 'hr9',
                'bb9': 'bb9', 'so9': 'so9', 'so_bb': 'so_w'
            }

            stat_mapping = stat_mapping_pitcher if player_type == 'pitcher' else stat_mapping_hitter
            avg_key = stat_mapping.get(stat_key)

            if avg_key:
                avg_val = avg_dict.get(avg_key)
                player_val = player_data.get(stat_key)

                if avg_val is not None and player_val is not None:
                    try:
                        avg_float = float(avg_val)
                        player_float = float(player_val)

                        # Lower is better for these stats
                        lower_is_better_stats = {'era', 'fip', 'whip', 'h9', 'hr9', 'bb9'}

                        if stat_key in lower_is_better_stats:
                            if player_float < avg_float:
                                return '\x1b[32m\x1b[1m\x1b[3m', '\x1b[0m'  # Green for below avg
                            elif player_float > avg_float:
                                return '\x1b[38;5;208m\x1b[1m\x1b[3m', '\x1b[0m'  # Orange for above avg
                        else:
                            if player_float > avg_float:
                                return '\x1b[32m\x1b[1m\x1b[3m', '\x1b[0m'  # Green for above avg
                            elif player_float < avg_float:
                                return '\x1b[38;5;208m\x1b[1m\x1b[3m', '\x1b[0m'  # Orange for below avg
                    except (ValueError, TypeError):
                        pass

            return '', ''

        # Normal league leader logic
        if year not in league_leaders or stat_key not in league_leaders[year]:
            return '', ''

        leaders = league_leaders[year][stat_key]
        nl_leader = leaders.get('NL')
        al_leader = leaders.get('AL')

        leads_own_league = False
        if player_league == 'NL' and nl_leader:
            leads_own_league = nl_leader.get('player') == player_data.get('player')
        elif player_league == 'AL' and al_leader:
            leads_own_league = al_leader.get('player') == player_data.get('player')

        if not leads_own_league:
            return '', ''

        if player_type == 'pitcher':
            lower_is_better_check = {'era', 'fip', 'whip', 'h9', 'hr9', 'bb9'}
        else:
            lower_is_better_check = {}

        leads_mlb = False

        try:
            player_val = float(player_data.get(stat_key))
            other_league = 'AL' if player_league == 'NL' else 'NL'
            other_leader = leaders.get(other_league)

            if other_leader:
                other_val = float(other_leader.get(stat_key))
                if stat_key in lower_is_better_check:
                    leads_mlb = player_val < other_val
                else:
                    leads_mlb = player_val > other_val
            else:
                leads_mlb = True
        except (ValueError, TypeError):
            pass

        if leads_mlb:
            return '\x1b[1m\x1b[3m', '\x1b[0m'
        else:
            return '\x1b[1m', '\x1b[0m'

    def print_row(row, row_data):
        year = row_data.get('year')
        team = row_data.get('team', '')
        awards = row_data.get('awards', '')
        player_league = row_data.get('lg', '')

        season_color = None
        is_award_winner = False
        if awards:
            is_award_winner = bool(re.search(r'(MVP-1|CYA-1|ROY-1)(?=[A-Z]|$)', awards))
        is_all_star = awards and 'AS' in awards
        is_2tm_3tm = '2TM' in team or '3TM' in team
        is_traded_allstar_team = not is_2tm_3tm and year in traded_allstar_years
        is_regular_traded_team = not is_2tm_3tm and year in traded_years and year not in traded_allstar_years

        if is_award_winner:
            season_color = 'magenta'
        elif is_all_star or is_traded_allstar_team:
            season_color = 'yellow'
        elif is_regular_traded_team:
            season_color = 'bright_black'

        color_codes = {
            'magenta': '\x1b[35m',
            'yellow': '\x1b[33m',
            'bright_black': '\x1b[90m',
        }

        row_parts = []
        for col_idx, value in enumerate(row):
            label, key = all_stats[col_idx]

            padded_value = value.ljust(col_widths[col_idx])

            if key == 'year' and season_color:
                color_code = color_codes.get(season_color, '')
                formatted_value = f"{color_code}{padded_value}\x1b[0m"
            else:
                prefix, suffix = get_stat_formatting(year, player_league, row_data, key)
                formatted_value = f"{prefix}{padded_value}{suffix}"

            row_parts.append(formatted_value)

        line = "  ".join(row_parts)
        click.echo(line)

    gap_dict = {idx: msg for idx, msg in gap_rows}
    for row_idx, row in enumerate(historical_rows):
        if row_idx in gap_dict:
            click.echo(gap_dict[row_idx])
        print_row(row, historical_data[row_idx])

    if season_2025_rows and historical_rows:
        # Find the most recent historical year (not just the last in the list)
        most_recent_historical_year = max(row.get('year') for row in historical_data if row.get('year'))
        if most_recent_historical_year and most_recent_historical_year < 2024:
            missing_years = list(range(most_recent_historical_year + 1, 2025))
            gap_msg = f"[Did not play in {', '.join(map(str, missing_years))}]"
            click.echo(gap_msg)

    if season_2025_rows and historical_rows:
        click.echo()

    if season_2025_rows:
        for row_idx, row in enumerate(season_2025_rows):
            print_row(row, season_2025_data[row_idx])

    # Print comparison averages if in comparison mode
    if comparison_mode and comparison_averages:
        click.echo()
        # Calculate separator width: sum of column widths + spaces between columns (2 spaces * (n-1))
        separator_line_width = sum(col_widths) + (len(col_widths) - 1) * 2
        click.echo("-" * separator_line_width)

        comparison_label = "League Avg" if comparison_mode == 'league' else "Team Avg"

        for yr in sorted(comparison_averages.keys(), reverse=False):
            avg_dict = comparison_averages[yr]
            row_values = []

            for label, key in all_stats:
                if key == 'year':
                    row_values.append(f"{yr} {comparison_label}")
                elif key in ['age', 'team', 'lg', 'awards']:
                    row_values.append('')
                else:
                    avg_key = stat_mapping.get(key)
                    if avg_key and avg_key in avg_dict:
                        val = avg_dict[avg_key]
                        row_values.append(format_stat_value(val) if val is not None else '')
                    else:
                        row_values.append('')

            row_parts = []
            for col_idx, value in enumerate(row_values):
                padded_value = value.ljust(col_widths[col_idx])
                row_parts.append(padded_value)

            line = "  ".join(row_parts)
            click.echo(line)

    click.echo()

def compare_to_average_display(player_dict, avg_dict, player_name, comparison_name, year, player_type, stats, is_league):
    """Display player stats compared to team/league average with green highlighting"""
    # Map player stats to team stats (different column names)
    stat_mapping_hitter = {
        'g': 'g', 'pa': 'pa', 'ab': 'ab', 'r': 'r', 'h': 'h',
        'doubles': 'doubles', 'triples': 'triples', 'hr': 'hr', 'rbi': 'rbi',
        'sb': 'sb', 'cs': 'cs', 'bb': 'bb', 'so': 'so',
        'ba': 'ba', 'obp': 'obp', 'slg': 'slg', 'ops': 'ops',
        'ops_plus': 'ops_plus', 'tb': 'tb', 'gidp': 'gdp',
        'hbp': 'hbp', 'sh': 'sh', 'sf': 'sf', 'ibb': 'ibb'
    }

    stat_mapping_pitcher = {
        'w': 'w', 'l': 'l', 'w_l_pct': 'w_l_pct', 'era': 'era',
        'g': 'g', 'gs': 'gs', 'gf': 'gf', 'cg': 'cg',
        'sho': 'c_sho', 'sv': 'sv', 'ip': 'ip', 'h': 'h',
        'r': 'r', 'er': 'er', 'hr': 'hr', 'bb': 'bb',
        'ibb': 'ibb', 'so': 'so', 'hbp': 'hbp', 'bk': 'bk',
        'wp': 'wp', 'bf': 'bf', 'era_plus': 'era_plus',
        'fip': 'fip', 'whip': 'whip', 'h9': 'h9', 'hr9': 'hr9',
        'bb9': 'bb9', 'so9': 'so9', 'so_bb': 'so_w'
    }

    stat_mapping = stat_mapping_pitcher if player_type == 'pitcher' else stat_mapping_hitter

    # Determine which stats to display
    if stats:
        stat_list = []
        for stat in stats:
            stat_lower = stat.lower()
            if stat_lower == 'w-l%':
                stat_key = 'w_l_pct'
                stat_label = 'W-L%'
            elif stat_lower == 'so/bb':
                stat_key = 'so_bb'
                stat_label = 'SO/BB'
            elif stat_lower == 'era+':
                stat_key = 'era_plus'
                stat_label = 'ERA+'
            elif stat_lower == 'ops+':
                stat_key = 'ops_plus'
                stat_label = 'OPS+'
            elif stat_lower == '2b':
                stat_key = 'doubles'
                stat_label = '2B'
            elif stat_lower == '3b':
                stat_key = 'triples'
                stat_label = '3B'
            elif stat_lower == 'h/9':
                stat_key = 'h9'
                stat_label = 'H/9'
            elif stat_lower == 'hr/9':
                stat_key = 'hr9'
                stat_label = 'HR/9'
            elif stat_lower == 'bb/9':
                stat_key = 'bb9'
                stat_label = 'BB/9'
            elif stat_lower == 'so/9':
                stat_key = 'so9'
                stat_label = 'SO/9'
            else:
                stat_key = stat_lower.replace('-', '_').replace('/', '_')
                stat_label = stat.upper()

            if stat_key in stat_mapping:
                stat_list.append((stat_label, stat_key))
    else:
        # Default stats - only rate stats and plus stats, no cumulative
        if player_type == 'pitcher':
            stat_list = [
                ('ERA', 'era'), ('WHIP', 'whip'), ('FIP', 'fip'),
                ('ERA+', 'era_plus'), ('W-L%', 'w_l_pct'),
                ('H/9', 'h9'), ('HR/9', 'hr9'), ('BB/9', 'bb9'),
                ('SO/9', 'so9'), ('SO/BB', 'so_bb')
            ]
        else:
            stat_list = [
                ('BA', 'ba'), ('OBP', 'obp'), ('SLG', 'slg'),
                ('OPS', 'ops'), ('OPS+', 'ops_plus')
            ]

    # Cumulative stats that need to be normalized
    cumulative_stats = {'r', 'h', 'doubles', 'triples', 'hr', 'rbi', 'sb', 'cs', 'bb', 'so', 'tb', 'hbp', 'sh', 'sf', 'ibb',
                        'w', 'l', 'gs', 'gf', 'cg', 'sho', 'sv', 'er', 'hbp', 'bk', 'wp'}

    lower_is_better = {'era', 'fip', 'whip', 'h9', 'hr9', 'bb9'}

    # Get normalization factor
    if player_type == 'hitter':
        players_used = avg_dict.get('bat_count', 1)
    else:
        players_used = avg_dict.get('pitcher_count', 1)

    # Print header
    click.echo(f"\n{player_name} vs {comparison_name} ({year})")
    click.echo("=" * 80)

    # Calculate column widths
    stat_col_width = max(len(label) for label, _ in stat_list)
    player_col_width = len(player_name)
    avg_col_width = len(comparison_name)

    # Print column headers
    header = f"{'Stat'.ljust(stat_col_width)}  {player_name.ljust(player_col_width)}  {comparison_name.ljust(avg_col_width)}"
    click.echo(header)
    click.echo("-" * len(header))

    # Green color codes
    green = '\x1b[32m'
    bold = '\x1b[1m'
    italic = '\x1b[3m'
    reset = '\x1b[0m'

    # Print each stat
    for stat_label, stat_key in stat_list:
        player_val = player_dict.get(stat_key)
        avg_key = stat_mapping.get(stat_key)
        avg_val = avg_dict.get(avg_key) if avg_key else None

        # Normalize cumulative stats
        if avg_val is not None and stat_key in cumulative_stats and players_used > 0:
            try:
                avg_val = float(avg_val) / players_used
            except (ValueError, TypeError):
                pass

        player_str = format_stat_value(player_val) if player_val is not None else 'N/A'
        avg_str = format_stat_value(avg_val) if avg_val is not None else 'N/A'

        player_formatted = player_str

        # Determine if player is above average (highlight in green)
        if player_val is not None and avg_val is not None and player_str != 'N/A' and avg_str != 'N/A':
            try:
                player_float = float(player_val)
                avg_float = float(avg_val)

                if stat_key in lower_is_better:
                    if player_float < avg_float:
                        player_formatted = f"{green}{bold}{italic}{player_str}{reset}"
                else:
                    if player_float > avg_float:
                        player_formatted = f"{green}{bold}{italic}{player_str}{reset}"
            except (ValueError, TypeError):
                pass

        player_display_len = len(player_str)
        avg_display_len = len(avg_str)

        line = f"{stat_label.ljust(stat_col_width)}  {player_formatted}{' ' * (player_col_width - player_display_len)}  {avg_str}{' ' * (avg_col_width - avg_display_len)}"
        click.echo(line)

    click.echo()

def compare_to_team(cursor, player_name, stats, year):
    """Compare a player's stats to their team average"""
    pitcher_matches, hitter_matches = find_player(cursor, player_name)

    if not pitcher_matches and not hitter_matches:
        click.echo(f"Error: No players found matching '{player_name}'")
        return

    if pitcher_matches and hitter_matches:
        # Check if this is Shohei Ohtani
        cursor.execute(f"SELECT * FROM pitcher_stats LIMIT 1")
        column_names = [desc[0] for desc in cursor.description]
        player_col_idx = column_names.index('player')
        first_pitcher = pitcher_matches[0]
        player_name_normalized = re.sub(r'[*#+]', '', first_pitcher[player_col_idx]).strip().lower()

        is_ohtani = 'shohei ohtani' in player_name_normalized

        if is_ohtani:
            click.echo("Error: Ohtani is a two-way player. Team comparison not supported for two-way players.")
            return

        # Not Ohtani - determine primary position by total games
        cursor.execute(f"SELECT * FROM pitcher_stats LIMIT 1")
        pitcher_column_names = [desc[0] for desc in cursor.description]
        pitcher_g_idx = pitcher_column_names.index('g')
        pitcher_total_games = sum(match[pitcher_g_idx] for match in pitcher_matches if match[pitcher_g_idx] is not None)

        cursor.execute(f"SELECT * FROM hitter_stats LIMIT 1")
        hitter_column_names = [desc[0] for desc in cursor.description]
        hitter_g_idx = hitter_column_names.index('g')
        hitter_total_games = sum(match[hitter_g_idx] for match in hitter_matches if match[hitter_g_idx] is not None)

        # Use whichever has more games
        if pitcher_total_games >= hitter_total_games:
            player_type = 'pitcher'
            matches = pitcher_matches
        else:
            player_type = 'hitter'
            matches = hitter_matches
    else:
        player_type = 'pitcher' if pitcher_matches else 'hitter'
        matches = pitcher_matches if pitcher_matches else hitter_matches

    # Use render_player with comparison_mode='team'
    render_player(cursor, matches, player_type, stats, year, comparison_mode='team')

def compare_to_league(cursor, player_name, stats, year):
    """Compare a player's stats to league average"""
    pitcher_matches, hitter_matches = find_player(cursor, player_name)

    if not pitcher_matches and not hitter_matches:
        click.echo(f"Error: No players found matching '{player_name}'")
        return

    if pitcher_matches and hitter_matches:
        # Check if this is Shohei Ohtani
        cursor.execute(f"SELECT * FROM pitcher_stats LIMIT 1")
        column_names = [desc[0] for desc in cursor.description]
        player_col_idx = column_names.index('player')
        first_pitcher = pitcher_matches[0]
        player_name_normalized = re.sub(r'[*#+]', '', first_pitcher[player_col_idx]).strip().lower()

        is_ohtani = 'shohei ohtani' in player_name_normalized

        if is_ohtani:
            click.echo("Error: Ohtani is a two-way player. League comparison not supported for two-way players.")
            return

        # Not Ohtani - determine primary position by total games
        cursor.execute(f"SELECT * FROM pitcher_stats LIMIT 1")
        pitcher_column_names = [desc[0] for desc in cursor.description]
        pitcher_g_idx = pitcher_column_names.index('g')
        pitcher_total_games = sum(match[pitcher_g_idx] for match in pitcher_matches if match[pitcher_g_idx] is not None)

        cursor.execute(f"SELECT * FROM hitter_stats LIMIT 1")
        hitter_column_names = [desc[0] for desc in cursor.description]
        hitter_g_idx = hitter_column_names.index('g')
        hitter_total_games = sum(match[hitter_g_idx] for match in hitter_matches if match[hitter_g_idx] is not None)

        # Use whichever has more games
        if pitcher_total_games >= hitter_total_games:
            player_type = 'pitcher'
            matches = pitcher_matches
        else:
            player_type = 'hitter'
            matches = hitter_matches
    else:
        player_type = 'pitcher' if pitcher_matches else 'hitter'
        matches = pitcher_matches if pitcher_matches else hitter_matches

    # Use render_player with comparison_mode='league'
    render_player(cursor, matches, player_type, stats, year, comparison_mode='league')

def compare_players(cursor, player1_name, player2_name, stats, year):
    """Compare two players' stats side by side"""
    # Find both players
    pitcher1_matches, hitter1_matches = find_player(cursor, player1_name)
    pitcher2_matches, hitter2_matches = find_player(cursor, player2_name)

    # Check if players were found
    if not pitcher1_matches and not hitter1_matches:
        click.echo(f"Error: No players found matching '{player1_name}'")
        return
    if not pitcher2_matches and not hitter2_matches:
        click.echo(f"Error: No players found matching '{player2_name}'")
        return

    # Determine player types
    player1_is_twoway = bool(pitcher1_matches and hitter1_matches)
    player2_is_twoway = bool(pitcher2_matches and hitter2_matches)

    # If no year specified, default to 2025
    if not year:
        year_filter = 2025
    else:
        if len(year) == 2 and year.isdigit():
            year_filter = int(f"20{year}")
        elif len(year) == 4 and year.isdigit():
            year_filter = int(year)
        else:
            click.echo(f"Error: Invalid year format '{year}'. Use 2022 or 22.")
            return

    # Determine which type of comparison to do
    # For simplicity, if both are pitchers or both are hitters, compare that type
    # If one is two-way, prefer pitcher comparison if other is pitcher, else hitter
    player1_type = None
    player2_type = None

    if pitcher1_matches and pitcher2_matches:
        player1_type = 'pitcher'
        player2_type = 'pitcher'
        player1_matches = pitcher1_matches
        player2_matches = pitcher2_matches
    elif hitter1_matches and hitter2_matches:
        player1_type = 'hitter'
        player2_type = 'hitter'
        player1_matches = hitter1_matches
        player2_matches = hitter2_matches
    else:
        click.echo(f"Error: Cannot compare players of different types (pitcher vs hitter)")
        return

    # Get column names
    cursor.execute(f"SELECT * FROM {player1_type}_stats LIMIT 1")
    column_names = [desc[0] for desc in cursor.description]

    # Check for multiple players for player 1
    player_col_idx = column_names.index('player')
    unique_players1 = set(match[player_col_idx] for match in player1_matches)
    if len(unique_players1) > 1:
        team_col_idx = column_names.index('team')
        year_col_idx = column_names.index('year')
        click.echo(f"Error: Multiple {player1_type}s found matching '{player1_name}':")
        for player in sorted(unique_players1):
            player_matches = [match for match in player1_matches if match[player_col_idx] == player]
            season_2025 = [m for m in player_matches if m[year_col_idx] == 2025]
            if season_2025:
                teams = [m[team_col_idx] for m in season_2025 if '2TM' not in m[team_col_idx] and '3TM' not in m[team_col_idx]]
                if not teams:
                    teams = [season_2025[0][team_col_idx]]
            else:
                most_recent_year = max(m[year_col_idx] for m in player_matches)
                recent_matches = [m for m in player_matches if m[year_col_idx] == most_recent_year]
                teams = [m[team_col_idx] for m in recent_matches if '2TM' not in m[team_col_idx] and '3TM' not in m[team_col_idx]]
                if not teams:
                    teams = [recent_matches[0][team_col_idx]]
            if len(teams) > 1:
                click.echo(f"  - {player} ({', '.join(teams)})")
            else:
                click.echo(f"  - {player} ({teams[0]})")
        return

    # Check for multiple players for player 2
    unique_players2 = set(match[player_col_idx] for match in player2_matches)
    if len(unique_players2) > 1:
        team_col_idx = column_names.index('team')
        year_col_idx = column_names.index('year')
        click.echo(f"Error: Multiple {player2_type}s found matching '{player2_name}':")
        for player in sorted(unique_players2):
            player_matches = [match for match in player2_matches if match[player_col_idx] == player]
            season_2025 = [m for m in player_matches if m[year_col_idx] == 2025]
            if season_2025:
                teams = [m[team_col_idx] for m in season_2025 if '2TM' not in m[team_col_idx] and '3TM' not in m[team_col_idx]]
                if not teams:
                    teams = [season_2025[0][team_col_idx]]
            else:
                most_recent_year = max(m[year_col_idx] for m in player_matches)
                recent_matches = [m for m in player_matches if m[year_col_idx] == most_recent_year]
                teams = [m[team_col_idx] for m in recent_matches if '2TM' not in m[team_col_idx] and '3TM' not in m[team_col_idx]]
                if not teams:
                    teams = [recent_matches[0][team_col_idx]]
            if len(teams) > 1:
                click.echo(f"  - {player} ({', '.join(teams)})")
            else:
                click.echo(f"  - {player} ({teams[0]})")
        return

    # Filter by year
    year_col_idx = column_names.index('year')
    player1_year_data = [m for m in player1_matches if m[year_col_idx] == year_filter]
    player2_year_data = [m for m in player2_matches if m[year_col_idx] == year_filter]

    if not player1_year_data:
        click.echo(f"Error: No data found for '{player1_name}' in {year_filter}")
        return
    if not player2_year_data:
        click.echo(f"Error: No data found for '{player2_name}' in {year_filter}")
        return

    # Handle multiple entries (e.g., traded players) - prefer 2TM/3TM entries as they have combined stats
    player1_data = None
    player2_data = None

    team_col_idx = column_names.index('team')

    # For player 1, prefer 2TM/3TM (combined stats) if available
    for entry in player1_year_data:
        if '2TM' in entry[team_col_idx] or '3TM' in entry[team_col_idx]:
            player1_data = entry
            break
    if not player1_data:
        player1_data = player1_year_data[0]

    # For player 2, prefer 2TM/3TM (combined stats) if available
    for entry in player2_year_data:
        if '2TM' in entry[team_col_idx] or '3TM' in entry[team_col_idx]:
            player2_data = entry
            break
    if not player2_data:
        player2_data = player2_year_data[0]

    # Convert to dicts
    player1_dict = dict(zip(column_names, player1_data))
    player2_dict = dict(zip(column_names, player2_data))

    # Get player names
    player1_full_name = player1_dict['player']
    player2_full_name = player2_dict['player']

    # Determine which stats to display
    if stats:
        # Custom stats
        stat_list = []
        for stat in stats:
            stat_lower = stat.lower()
            if stat_lower == 'w-l%':
                stat_key = 'w_l_pct'
                stat_label = 'W-L%'
            elif stat_lower == 'so/bb':
                stat_key = 'so_bb'
                stat_label = 'SO/BB'
            elif stat_lower == 'era+':
                stat_key = 'era_plus'
                stat_label = 'ERA+'
            elif stat_lower == 'ops+':
                stat_key = 'ops_plus'
                stat_label = 'OPS+'
            elif stat_lower == 'rbat+':
                stat_key = 'rbat_plus'
                stat_label = 'Rbat+'
            elif stat_lower == '2b':
                stat_key = 'doubles'
                stat_label = '2B'
            elif stat_lower == '3b':
                stat_key = 'triples'
                stat_label = '3B'
            elif stat_lower == 'h/9':
                stat_key = 'h9'
                stat_label = 'H/9'
            elif stat_lower == 'hr/9':
                stat_key = 'hr9'
                stat_label = 'HR/9'
            elif stat_lower == 'bb/9':
                stat_key = 'bb9'
                stat_label = 'BB/9'
            elif stat_lower == 'so/9':
                stat_key = 'so9'
                stat_label = 'SO/9'
            else:
                stat_key = stat_lower.replace('-', '_').replace('/', '_')
                stat_label = stat.upper()

            if stat_key in column_names:
                stat_list.append((stat_label, stat_key))
    else:
        # Default stats based on player type
        if player1_type == 'pitcher':
            stat_list = [
                ('WAR', 'war'), ('W', 'w'), ('L', 'l'), ('ERA', 'era'),
                ('G', 'g'), ('GS', 'gs'), ('IP', 'ip'), ('H', 'h'),
                ('R', 'r'), ('ER', 'er'), ('HR', 'hr'), ('BB', 'bb'),
                ('SO', 'so'), ('WHIP', 'whip'), ('ERA+', 'era_plus'),
                ('FIP', 'fip'), ('SO/9', 'so9'), ('BB/9', 'bb9')
            ]
        else:
            stat_list = [
                ('WAR', 'war'), ('G', 'g'), ('PA', 'pa'), ('AB', 'ab'),
                ('R', 'r'), ('H', 'h'), ('2B', 'doubles'), ('3B', 'triples'),
                ('HR', 'hr'), ('RBI', 'rbi'), ('SB', 'sb'), ('BB', 'bb'),
                ('SO', 'so'), ('BA', 'ba'), ('OBP', 'obp'), ('SLG', 'slg'),
                ('OPS', 'ops'), ('OPS+', 'ops_plus')
            ]

    lower_is_better = {'era', 'fip', 'whip', 'h9', 'hr9', 'bb9'}

    # Print header
    click.echo(f"\n{player1_full_name} vs {player2_full_name} ({year_filter})")
    click.echo("=" * 80)

    # Calculate column widths
    stat_col_width = max(len(label) for label, _ in stat_list)
    player1_col_width = len(player1_full_name)
    player2_col_width = len(player2_full_name)

    # Print column headers
    header = f"{'Stat'.ljust(stat_col_width)}  {player1_full_name.ljust(player1_col_width)}  {player2_full_name.ljust(player2_col_width)}"
    click.echo(header)
    click.echo("-" * len(header))

    # Green color code
    green = '\x1b[32m'
    bold = '\x1b[1m'
    italic = '\x1b[3m'
    reset = '\x1b[0m'

    # Print each stat
    for stat_label, stat_key in stat_list:
        val1 = player1_dict.get(stat_key)
        val2 = player2_dict.get(stat_key)

        # Format values
        val1_str = format_stat_value(val1) if val1 is not None else 'N/A'
        val2_str = format_stat_value(val2) if val2 is not None else 'N/A'

        # Determine which is better
        val1_formatted = val1_str
        val2_formatted = val2_str

        if val1 is not None and val2 is not None and val1_str != 'N/A' and val2_str != 'N/A':
            try:
                val1_float = float(val1)
                val2_float = float(val2)

                if stat_key in lower_is_better:
                    # Lower is better
                    if val1_float < val2_float:
                        val1_formatted = f"{green}{bold}{italic}{val1_str}{reset}"
                    elif val2_float < val1_float:
                        val2_formatted = f"{green}{bold}{italic}{val2_str}{reset}"
                else:
                    # Higher is better
                    if val1_float > val2_float:
                        val1_formatted = f"{green}{bold}{italic}{val1_str}{reset}"
                    elif val2_float > val1_float:
                        val2_formatted = f"{green}{bold}{italic}{val2_str}{reset}"
            except (ValueError, TypeError):
                pass

        # Print row - need to account for ANSI codes in padding
        val1_display_len = len(val1_str)
        val2_display_len = len(val2_str)

        line = f"{stat_label.ljust(stat_col_width)}  {val1_formatted}{' ' * (player1_col_width - val1_display_len)}  {val2_formatted}{' ' * (player2_col_width - val2_display_len)}"
        click.echo(line)

    click.echo()

def display_platoon_splits(cursor, player_name, year, stats):
    """Display platoon splits for a player"""
    pitcher_matches, hitter_matches = find_player(cursor, player_name)

    if not pitcher_matches and not hitter_matches:
        click.echo(f"Error: No players found matching '{player_name}'")
        return

    # Determine player type
    if pitcher_matches and hitter_matches:
        # Check if this is Shohei Ohtani
        cursor.execute(f"SELECT * FROM pitcher_stats LIMIT 1")
        column_names = [desc[0] for desc in cursor.description]
        player_col_idx = column_names.index('player')
        first_pitcher = pitcher_matches[0]
        player_name_normalized = re.sub(r'[*#+]', '', first_pitcher[player_col_idx]).strip().lower()

        is_ohtani = 'shohei ohtani' in player_name_normalized

        if is_ohtani:
            click.echo(f"Error: Ohtani is a two-way player. Platoon splits not yet supported for two-way players.")
            return

        # Not Ohtani - determine primary position by total games
        cursor.execute(f"SELECT * FROM pitcher_stats LIMIT 1")
        pitcher_column_names = [desc[0] for desc in cursor.description]
        pitcher_g_idx = pitcher_column_names.index('g')
        pitcher_total_games = sum(match[pitcher_g_idx] for match in pitcher_matches if match[pitcher_g_idx] is not None)

        cursor.execute(f"SELECT * FROM hitter_stats LIMIT 1")
        hitter_column_names = [desc[0] for desc in cursor.description]
        hitter_g_idx = hitter_column_names.index('g')
        hitter_total_games = sum(match[hitter_g_idx] for match in hitter_matches if match[hitter_g_idx] is not None)

        # Use whichever has more games
        if pitcher_total_games >= hitter_total_games:
            player_type = 'pitcher'
            matches = pitcher_matches
        else:
            player_type = 'hitter'
            matches = hitter_matches
    else:
        player_type = 'pitcher' if pitcher_matches else 'hitter'
        matches = pitcher_matches if pitcher_matches else hitter_matches

    # Check for multiple players
    cursor.execute(f"SELECT * FROM {player_type}_stats LIMIT 1")
    column_names = [desc[0] for desc in cursor.description]
    player_col_idx = column_names.index('player')
    unique_players = set(match[player_col_idx] for match in matches)

    if len(unique_players) > 1:
        click.echo(f"Error: Multiple {player_type}s found matching '{player_name}'")
        return

    full_name = matches[0][player_col_idx]
    clean_name = re.sub(r'[*#+]', '', full_name).strip()

    # Look up MLB ID
    player_info = mlb_api.lookup_player(clean_name)
    if not player_info:
        click.echo(f"Error: Could not find MLB ID for '{clean_name}'")
        return

    player_id = player_info['id']
    api_name = player_info['fullName']

    # Parse year
    year_filter = None
    all_years = False
    if year:
        if year.lower() == 'all':
            all_years = True
        elif len(year) == 2 and year.isdigit():
            year_filter = int(f"20{year}")
        elif len(year) == 4 and year.isdigit():
            year_filter = int(year)
        else:
            click.echo(f"Error: Invalid year format '{year}'. Use 2022, 22, or all.")
            return

    # Check cache first
    cached_splits = mlb_api.get_cached_platoon_splits(cursor, api_name, year_filter, all_years)

    if cached_splits:
        splits = cached_splits
    else:
        # Fetch from API
        click.echo(f"Fetching platoon splits from MLB Stats API...")
        splits = mlb_api.fetch_platoon_splits(player_id, player_type, year_filter, all_years)

        if not splits:
            year_str = f" for {year_filter}" if year_filter else ""
            click.echo(f"No platoon split data found for {api_name}{year_str}")
            return

        # Cache the results
        mlb_api.cache_platoon_splits(cursor, api_name, player_id, player_type, splits, year_filter, all_years)

    if all_years:
        # Year-by-year display
        if not isinstance(splits, list) or len(splits) == 0:
            click.echo(f"No platoon split data found for {api_name}")
            return
    else:
        # Single year/career display
        if 'left' not in splits or 'right' not in splits:
            year_str = f" for {year_filter}" if year_filter else ""
            click.echo(f"No platoon split data found for {api_name}{year_str}")
            return

    # Handle year-by-year display
    if all_years:
        # Display year-by-year platoon splits
        year_str = " (Year-by-Year)"
        click.echo(f"\n{api_name} - Platoon Splits{year_str}")
        click.echo("=" * 80)

        # Determine columns based on player type
        if player_type == 'pitcher':
            if stats:
                headers = ['Year', 'Split']
                for stat in stats:
                    stat_lower = stat.lower()
                    if stat_lower in ['avg', 'ba']: headers.append('AVG')
                    elif stat_lower == 'ops': headers.append('OPS')
                    elif stat_lower == 'hr': headers.append('HR')
                    elif stat_lower == 'so': headers.append('SO')
                    elif stat_lower == 'whip': headers.append('WHIP')
                    elif stat_lower == 'ip': headers.append('IP')
            else:
                headers = ['Year', 'Split', 'PA', 'AB', 'H', '2B', '3B', 'HR', 'BB', 'SO', 'AVG', 'OBP', 'SLG', 'OPS', 'IP']
        else:
            if stats:
                headers = ['Year', 'Split']
                for stat in stats:
                    stat_lower = stat.lower()
                    if stat_lower in ['avg', 'ba']: headers.append('AVG')
                    elif stat_lower == 'ops': headers.append('OPS')
                    elif stat_lower == 'hr': headers.append('HR')
                    elif stat_lower == 'rbi': headers.append('RBI')
            else:
                headers = ['Year', 'Split', 'PA', 'AB', 'H', '2B', '3B', 'HR', 'RBI', 'BB', 'SO', 'AVG', 'OBP', 'SLG', 'OPS']

        # Print header
        click.echo("\n" + "  ".join(f"{h:<6}" for h in headers))
        header_len = len("  ".join(f"{h:<6}" for h in headers))
        click.echo("-" * header_len)

        # Print data for each year
        for year_data in splits:
            yr = year_data['year']
            left_stat = year_data['left']
            right_stat = year_data['right']

            # vs Left row
            left_label = 'LHB' if player_type == 'pitcher' else 'LHP'
            row_parts = [f"{yr:<6}", f"{left_label:<6}"]

            if player_type == 'pitcher':
                if stats:
                    for stat in stats:
                        stat_lower = stat.lower()
                        if stat_lower in ['avg', 'ba']: row_parts.append(f"{left_stat['ba']:<6.3f}")
                        elif stat_lower == 'ops': row_parts.append(f"{left_stat['ops']:<6.3f}")
                        elif stat_lower == 'hr': row_parts.append(f"{left_stat['hr']:<6}")
                        elif stat_lower == 'so': row_parts.append(f"{left_stat['so']:<6}")
                        elif stat_lower == 'whip': row_parts.append(f"{left_stat['whip']:<6.3f}")
                        elif stat_lower == 'ip': row_parts.append(f"{left_stat['ip']:<6}")
                else:
                    row_parts.extend([
                        f"{left_stat['pa']:<6}",
                        f"{left_stat['ab']:<6}",
                        f"{left_stat['h']:<6}",
                        f"{left_stat['doubles']:<6}",
                        f"{left_stat['triples']:<6}",
                        f"{left_stat['hr']:<6}",
                        f"{left_stat['bb']:<6}",
                        f"{left_stat['so']:<6}",
                        f"{left_stat['ba']:<6.3f}",
                        f"{left_stat['obp']:<6.3f}",
                        f"{left_stat['slg']:<6.3f}",
                        f"{left_stat['ops']:<6.3f}",
                        f"{left_stat['ip']:<6}"
                    ])
            else:
                if stats:
                    for stat in stats:
                        stat_lower = stat.lower()
                        if stat_lower in ['avg', 'ba']: row_parts.append(f"{left_stat['ba']:<6.3f}")
                        elif stat_lower == 'ops': row_parts.append(f"{left_stat['ops']:<6.3f}")
                        elif stat_lower == 'hr': row_parts.append(f"{left_stat['hr']:<6}")
                        elif stat_lower == 'rbi': row_parts.append(f"{left_stat['rbi']:<6}")
                else:
                    row_parts.extend([
                        f"{left_stat['pa']:<6}",
                        f"{left_stat['ab']:<6}",
                        f"{left_stat['h']:<6}",
                        f"{left_stat['doubles']:<6}",
                        f"{left_stat['triples']:<6}",
                        f"{left_stat['hr']:<6}",
                        f"{left_stat['rbi']:<6}",
                        f"{left_stat['bb']:<6}",
                        f"{left_stat['so']:<6}",
                        f"{left_stat['ba']:<6.3f}",
                        f"{left_stat['obp']:<6.3f}",
                        f"{left_stat['slg']:<6.3f}",
                        f"{left_stat['ops']:<6.3f}"
                    ])

            click.echo("  ".join(row_parts))

            # vs Right row
            right_label = 'RHB' if player_type == 'pitcher' else 'RHP'
            row_parts = [f"{'':<6}", f"{right_label:<6}"]

            if player_type == 'pitcher':
                if stats:
                    for stat in stats:
                        stat_lower = stat.lower()
                        if stat_lower in ['avg', 'ba']: row_parts.append(f"{right_stat['ba']:<6.3f}")
                        elif stat_lower == 'ops': row_parts.append(f"{right_stat['ops']:<6.3f}")
                        elif stat_lower == 'hr': row_parts.append(f"{right_stat['hr']:<6}")
                        elif stat_lower == 'so': row_parts.append(f"{right_stat['so']:<6}")
                        elif stat_lower == 'whip': row_parts.append(f"{right_stat['whip']:<6.3f}")
                        elif stat_lower == 'ip': row_parts.append(f"{right_stat['ip']:<6}")
                else:
                    row_parts.extend([
                        f"{right_stat['pa']:<6}",
                        f"{right_stat['ab']:<6}",
                        f"{right_stat['h']:<6}",
                        f"{right_stat['doubles']:<6}",
                        f"{right_stat['triples']:<6}",
                        f"{right_stat['hr']:<6}",
                        f"{right_stat['bb']:<6}",
                        f"{right_stat['so']:<6}",
                        f"{right_stat['ba']:<6.3f}",
                        f"{right_stat['obp']:<6.3f}",
                        f"{right_stat['slg']:<6.3f}",
                        f"{right_stat['ops']:<6.3f}",
                        f"{right_stat['ip']:<6}"
                    ])
            else:
                if stats:
                    for stat in stats:
                        stat_lower = stat.lower()
                        if stat_lower in ['avg', 'ba']: row_parts.append(f"{right_stat['ba']:<6.3f}")
                        elif stat_lower == 'ops': row_parts.append(f"{right_stat['ops']:<6.3f}")
                        elif stat_lower == 'hr': row_parts.append(f"{right_stat['hr']:<6}")
                        elif stat_lower == 'rbi': row_parts.append(f"{right_stat['rbi']:<6}")
                else:
                    row_parts.extend([
                        f"{right_stat['pa']:<6}",
                        f"{right_stat['ab']:<6}",
                        f"{right_stat['h']:<6}",
                        f"{right_stat['doubles']:<6}",
                        f"{right_stat['triples']:<6}",
                        f"{right_stat['hr']:<6}",
                        f"{right_stat['rbi']:<6}",
                        f"{right_stat['bb']:<6}",
                        f"{right_stat['so']:<6}",
                        f"{right_stat['ba']:<6.3f}",
                        f"{right_stat['obp']:<6.3f}",
                        f"{right_stat['slg']:<6.3f}",
                        f"{right_stat['ops']:<6.3f}"
                    ])

            click.echo("  ".join(row_parts))

        click.echo()
        return

    # Single year or career display
    left_stat = splits['left']
    right_stat = splits['right']

    # Determine which stats to display
    if player_type == 'pitcher':
        if stats:
            # Custom stats
            all_stats = [('Split', None)]
            for stat in stats:
                stat_lower = stat.lower()
                if stat_lower in ['pa', 'bf']:
                    all_stats.append(('PA', 'pa'))
                elif stat_lower == 'ab':
                    all_stats.append(('AB', 'ab'))
                elif stat_lower == 'h':
                    all_stats.append(('H', 'h'))
                elif stat_lower in ['2b', 'doubles']:
                    all_stats.append(('2B', 'doubles'))
                elif stat_lower in ['3b', 'triples']:
                    all_stats.append(('3B', 'triples'))
                elif stat_lower == 'hr':
                    all_stats.append(('HR', 'hr'))
                elif stat_lower == 'bb':
                    all_stats.append(('BB', 'bb'))
                elif stat_lower == 'so':
                    all_stats.append(('SO', 'so'))
                elif stat_lower in ['ba', 'avg']:
                    all_stats.append(('AVG', 'ba'))
                elif stat_lower == 'obp':
                    all_stats.append(('OBP', 'obp'))
                elif stat_lower == 'slg':
                    all_stats.append(('SLG', 'slg'))
                elif stat_lower == 'ops':
                    all_stats.append(('OPS', 'ops'))
                elif stat_lower == 'ip':
                    all_stats.append(('IP', 'ip'))
                elif stat_lower == 'whip':
                    all_stats.append(('WHIP', 'whip'))
                elif stat_lower == 'era':
                    all_stats.append(('ERA', 'era'))
                elif stat_lower in ['k/9', 'so/9', 'k9', 'so9']:
                    all_stats.append(('K/9', 'k9'))
                elif stat_lower in ['bb/9', 'bb9']:
                    all_stats.append(('BB/9', 'bb9'))
        else:
            # Default stats for pitchers
            all_stats = [
                ('Split', None), ('PA', 'pa'), ('AB', 'ab'), ('H', 'h'),
                ('2B', 'doubles'), ('3B', 'triples'), ('HR', 'hr'), ('BB', 'bb'),
                ('SO', 'so'), ('AVG', 'ba'), ('OBP', 'obp'), ('SLG', 'slg'),
                ('OPS', 'ops'), ('IP', 'ip')
            ]
    else:  # hitter
        if stats:
            # Custom stats
            all_stats = [('Split', None)]
            for stat in stats:
                stat_lower = stat.lower()
                if stat_lower == 'pa':
                    all_stats.append(('PA', 'pa'))
                elif stat_lower == 'ab':
                    all_stats.append(('AB', 'ab'))
                elif stat_lower == 'h':
                    all_stats.append(('H', 'h'))
                elif stat_lower in ['2b', 'doubles']:
                    all_stats.append(('2B', 'doubles'))
                elif stat_lower in ['3b', 'triples']:
                    all_stats.append(('3B', 'triples'))
                elif stat_lower == 'hr':
                    all_stats.append(('HR', 'hr'))
                elif stat_lower == 'rbi':
                    all_stats.append(('RBI', 'rbi'))
                elif stat_lower == 'bb':
                    all_stats.append(('BB', 'bb'))
                elif stat_lower == 'so':
                    all_stats.append(('SO', 'so'))
                elif stat_lower in ['ba', 'avg']:
                    all_stats.append(('AVG', 'ba'))
                elif stat_lower == 'obp':
                    all_stats.append(('OBP', 'obp'))
                elif stat_lower == 'slg':
                    all_stats.append(('SLG', 'slg'))
                elif stat_lower == 'ops':
                    all_stats.append(('OPS', 'ops'))
        else:
            # Default stats for hitters
            all_stats = [
                ('Split', None), ('PA', 'pa'), ('AB', 'ab'), ('H', 'h'),
                ('2B', 'doubles'), ('3B', 'triples'), ('HR', 'hr'), ('RBI', 'rbi'),
                ('BB', 'bb'), ('SO', 'so'), ('AVG', 'ba'), ('OBP', 'obp'),
                ('SLG', 'slg'), ('OPS', 'ops')
            ]

    # Display
    year_str = f" ({year_filter})" if year_filter else " (Career)"
    click.echo(f"\n{api_name} - Platoon Splits{year_str}")
    click.echo("=" * 80)

    # Build header
    header_parts = []
    for label, key in all_stats:
        if key is None:
            header_parts.append(f"{label:<15}")
        elif key in ['ba', 'obp', 'slg', 'ops', 'whip', 'era', 'k9', 'bb9']:
            header_parts.append(f"{label:<7}")
        elif key == 'ip':
            header_parts.append(f"{label:<7}")
        elif key == 'rbi':
            header_parts.append(f"{label:<5}")
        else:
            header_parts.append(f"{label:<6}")

    click.echo("\n" + " ".join(header_parts))
    click.echo("-" * 100)

    # Build rows for vs Left and vs Right
    for split_name, split_data in [('vs LHB' if player_type == 'pitcher' else 'vs LHP', left_stat),
                                     ('vs RHB' if player_type == 'pitcher' else 'vs RHP', right_stat)]:
        row_parts = []
        for label, key in all_stats:
            if key is None:
                row_parts.append(f"{split_name:<15}")
            elif key in ['ba', 'obp', 'slg', 'ops', 'whip', 'era', 'k9', 'bb9']:
                val = split_data.get(key, 0.0)
                row_parts.append(f"{val:<7.3f}")
            elif key == 'ip':
                val = split_data.get(key, '0')
                row_parts.append(f"{val:<7}")
            elif key == 'rbi':
                val = split_data.get(key, 0)
                row_parts.append(f"{val:<5}")
            else:
                val = split_data.get(key, 0)
                row_parts.append(f"{val:<6}")
        click.echo(" ".join(row_parts))

    click.echo()

def handle_versus_matchup(cursor, player1_name, player2_name, year_filter):
    """Handle batter vs pitcher matchup display"""
    # Look up both players in our database to understand their types
    pitcher1_matches, hitter1_matches = find_player(cursor, player1_name)
    pitcher2_matches, hitter2_matches = find_player(cursor, player2_name)

    # Check if players were found
    if not pitcher1_matches and not hitter1_matches:
        click.echo(f"Error: No players found matching '{player1_name}'")
        return
    if not pitcher2_matches and not hitter2_matches:
        click.echo(f"Error: No players found matching '{player2_name}'")
        return

    # Determine player types
    player1_is_pitcher = bool(pitcher1_matches)
    player1_is_hitter = bool(hitter1_matches)
    player2_is_pitcher = bool(pitcher2_matches)
    player2_is_hitter = bool(hitter2_matches)

    # Get full player names from database
    cursor.execute(f"SELECT * FROM pitcher_stats LIMIT 1")
    column_names = [desc[0] for desc in cursor.description]
    player_col_idx = column_names.index('player')

    if pitcher1_matches or hitter1_matches:
        matches1 = pitcher1_matches if pitcher1_matches else hitter1_matches
        player1_full_name = matches1[0][player_col_idx]
    else:
        player1_full_name = player1_name

    if pitcher2_matches or hitter2_matches:
        matches2 = pitcher2_matches if pitcher2_matches else hitter2_matches
        player2_full_name = matches2[0][player_col_idx]
    else:
        player2_full_name = player2_name

    # Clean names for API lookup (remove asterisks and other special characters from Baseball Reference)
    def clean_player_name(name):
        # Remove * # + and other special characters that Baseball Reference uses
        return re.sub(r'[*#+]', '', name).strip()

    player1_clean_name = clean_player_name(player1_full_name)
    player2_clean_name = clean_player_name(player2_full_name)

    # Determine batter and pitcher roles
    batter_full_name = None
    pitcher_full_name = None
    batter_clean_name = None
    pitcher_clean_name = None

    if player1_is_hitter and not player1_is_pitcher and player2_is_pitcher:
        # Player 1 is only a hitter, Player 2 is pitcher
        batter_full_name = player1_full_name
        batter_clean_name = player1_clean_name
        pitcher_full_name = player2_full_name
        pitcher_clean_name = player2_clean_name
    elif player1_is_pitcher and player2_is_hitter and not player2_is_pitcher:
        # Player 1 is pitcher, Player 2 is only a hitter
        batter_full_name = player2_full_name
        batter_clean_name = player2_clean_name
        pitcher_full_name = player1_full_name
        pitcher_clean_name = player1_clean_name
    elif player1_is_hitter and player2_is_pitcher and not player2_is_hitter:
        # Player 1 can hit, Player 2 is only a pitcher
        batter_full_name = player1_full_name
        batter_clean_name = player1_clean_name
        pitcher_full_name = player2_full_name
        pitcher_clean_name = player2_clean_name
    elif player1_is_pitcher and not player1_is_hitter and player2_is_hitter:
        # Player 1 is only a pitcher, Player 2 can hit
        batter_full_name = player2_full_name
        batter_clean_name = player2_clean_name
        pitcher_full_name = player1_full_name
        pitcher_clean_name = player1_clean_name
    elif player1_is_hitter and player1_is_pitcher and player2_is_hitter and player2_is_pitcher:
        # Both are two-way players - ask user
        click.echo(f"Both {player1_full_name} and {player2_full_name} are two-way players.")
        click.echo(f"Who is batting?")
        click.echo(f"  1. {player1_full_name}")
        click.echo(f"  2. {player2_full_name}")
        choice = click.prompt("Enter 1 or 2", type=int)
        if choice == 1:
            batter_full_name = player1_full_name
            batter_clean_name = player1_clean_name
            pitcher_full_name = player2_full_name
            pitcher_clean_name = player2_clean_name
        else:
            batter_full_name = player2_full_name
            batter_clean_name = player2_clean_name
            pitcher_full_name = player1_full_name
            pitcher_clean_name = player1_clean_name
    else:
        click.echo(f"Error: Could not determine batter/pitcher roles for '{player1_name}' vs '{player2_name}'")
        click.echo(f"  {player1_name}: {'pitcher' if player1_is_pitcher else ''} {'hitter' if player1_is_hitter else ''}")
        click.echo(f"  {player2_name}: {'pitcher' if player2_is_pitcher else ''} {'hitter' if player2_is_hitter else ''}")
        return

    # Check cache first (fast - no API calls needed)
    # Try both cleaned and original names since we may have cached with either
    cached_stats = None
    for batter_search in [batter_clean_name, batter_full_name]:
        for pitcher_search in [pitcher_clean_name, pitcher_full_name]:
            cached_stats = mlb_api.get_cached_matchup(cursor, batter_search, pitcher_search)
            if cached_stats:
                # Use the cached names for display
                render_matchup_stats(batter_search, pitcher_search, cached_stats, year_filter)
                return

    # Not cached - need to look up MLB IDs and fetch from API (use cleaned names)
    batter_info = mlb_api.lookup_player(batter_clean_name)
    pitcher_info = mlb_api.lookup_player(pitcher_clean_name)

    if not batter_info:
        click.echo(f"Error: Could not find MLB ID for '{batter_clean_name}'")
        return
    if not pitcher_info:
        click.echo(f"Error: Could not find MLB ID for '{pitcher_clean_name}'")
        return

    batter_api_name = batter_info['fullName']
    pitcher_api_name = pitcher_info['fullName']
    batter_id = batter_info['id']
    pitcher_id = pitcher_info['id']

    # Fetch from API
    click.echo(f"Fetching matchup data from MLB Stats API...")
    stats_list = mlb_api.fetch_batter_vs_pitcher_stats(batter_id, pitcher_id)

    if not stats_list:
        click.echo(f"No matchup data found for {batter_api_name} vs {pitcher_api_name}")
        return

    # Cache the results
    mlb_api.cache_matchup(cursor, batter_api_name, batter_id, pitcher_api_name, pitcher_id, stats_list)

    # Display the stats
    render_matchup_stats(batter_api_name, pitcher_api_name, stats_list, year_filter)

def render_matchup_stats(batter_name, pitcher_name, stats_list, year_filter):
    """Render batter vs pitcher matchup stats"""
    # Filter stats based on year_filter
    if year_filter and year_filter.lower() == 'all':
        # Show year-by-year breakdown
        display_stats = [s for s in stats_list if s['year'] != 'career']
        career_total = [s for s in stats_list if s['year'] == 'career']
    elif year_filter:
        # Show specific year
        if len(year_filter) == 2 and year_filter.isdigit():
            year_str = f"20{year_filter}"
        else:
            year_str = year_filter

        display_stats = [s for s in stats_list if s['year'] == year_str]
        career_total = []

        if not display_stats:
            click.echo(f"No matchup data found for {year_str}")
            return
    else:
        # Show career totals only (default)
        display_stats = [s for s in stats_list if s['year'] == 'career']
        career_total = []

    if not display_stats:
        click.echo(f"No matchup data found")
        return

    # Determine title
    if year_filter and year_filter.lower() == 'all':
        title = f"{batter_name} vs {pitcher_name} (Career Breakdown)"
    elif year_filter:
        if len(year_filter) == 2 and year_filter.isdigit():
            year_display = f"20{year_filter}"
        else:
            year_display = year_filter
        title = f"{batter_name} vs {pitcher_name} ({year_display})"
    else:
        title = f"{batter_name} vs {pitcher_name} (Career)"

    click.echo(f"\n{title}")
    click.echo("=" * 80)

    # Display format
    if year_filter and year_filter.lower() == 'all':
        # Year-by-year table - similar to regular player stats
        all_stats = [
            ('Year', 'year'), ('G', 'games'), ('PA', 'pa'), ('AB', 'ab'),
            ('H', 'h'), ('2B', 'doubles'), ('3B', 'triples'), ('HR', 'hr'),
            ('RBI', 'rbi'), ('BB', 'bb'), ('SO', 'so'), ('HBP', 'hbp'),
            ('IBB', 'ibb'), ('AVG', 'ba'), ('OBP', 'obp'), ('SLG', 'slg'), ('OPS', 'ops')
        ]

        # Calculate column widths based on header and data
        col_widths = []
        for label, key in all_stats:
            max_width = len(label)
            for stat in display_stats:
                if key == 'year':
                    val_str = str(stat[key])
                elif key in ['ba', 'obp', 'slg', 'ops']:
                    val_str = f"{stat[key]:.3f}" if stat[key] else '.000'
                else:
                    val_str = str(stat[key])
                max_width = max(max_width, len(val_str))
            col_widths.append(max_width)

        # Print header
        header_parts = []
        for idx, (label, key) in enumerate(all_stats):
            header_parts.append(label.ljust(col_widths[idx]))
        click.echo("  ".join(header_parts))

        # Print each year
        for stat in display_stats:
            row_parts = []
            for idx, (label, key) in enumerate(all_stats):
                if key == 'year':
                    val = str(stat[key])
                elif key in ['ba', 'obp', 'slg', 'ops']:
                    val = f"{stat[key]:.3f}" if stat[key] else '.000'
                else:
                    val = str(stat[key])
                row_parts.append(val.ljust(col_widths[idx]))
            click.echo("  ".join(row_parts))

    else:
        # Single stat summary
        stat = display_stats[0]
        stat_labels = [
            ('Games', stat['games']),
            ('PA', stat['pa']),
            ('AB', stat['ab']),
            ('H', stat['h']),
            ('2B', stat['doubles']),
            ('3B', stat['triples']),
            ('HR', stat['hr']),
            ('RBI', stat['rbi']),
            ('BB', stat['bb']),
            ('HBP', stat['hbp']),
            ('SO', stat['so']),
            ('IBB', stat['ibb']),
            ('AVG', f"{stat['ba']:.3f}" if stat['ba'] else '.000'),
            ('OBP', f"{stat['obp']:.3f}" if stat['obp'] else '.000'),
            ('SLG', f"{stat['slg']:.3f}" if stat['slg'] else '.000'),
            ('OPS', f"{stat['ops']:.3f}" if stat['ops'] else '.000'),
        ]

        label_width = max(len(label) for label, _ in stat_labels)
        for label, value in stat_labels:
            click.echo(f"{label.ljust(label_width)}  {value}")

    click.echo()

@click.command()
@click.argument('player_name')
@click.option('-s', '--stats', multiple=True, help='Specific stats to display (e.g., war era)')
@click.option('-y', '--year', help='Filter by year (e.g., 2022 or 22)')
@click.option('-c', '--compare', help='Compare with another player')
@click.option('-ct', '--compare-team', is_flag=True, help='Compare player to team average')
@click.option('-cl', '--compare-league', is_flag=True, help='Compare player to league average')
@click.option('-v', '--versus', help='Show batter vs pitcher matchup stats')
@click.option('-p', '--platoon', is_flag=True, help='Show platoon splits (vs LHB/RHB or vs LHP/RHP)')
def main(player_name, stats, year, compare, compare_team, compare_league, versus, platoon):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Validate that conflicting flags aren't used together
        if sum([bool(compare), compare_team, compare_league, bool(versus), platoon]) > 1:
            click.echo("Error: Cannot use -c, -ct, -cl, -v, and -p together. Choose one mode.")
            return

        # If platoon flag is set, show platoon splits
        if platoon:
            display_platoon_splits(cursor, player_name, year, stats)
            cursor.close()
            conn.close()
            return

        # If versus flag is set, show batter vs pitcher matchup
        if versus:
            handle_versus_matchup(cursor, player_name, versus, year)
            cursor.close()
            conn.close()
            return

        # If compare flag is set, use comparison mode
        if compare:
            compare_players(cursor, player_name, compare, stats, year)
            cursor.close()
            conn.close()
            return

        # If compare-team flag is set, compare to team average
        if compare_team:
            compare_to_team(cursor, player_name, stats, year)
            cursor.close()
            conn.close()
            return

        # If compare-league flag is set, compare to league average
        if compare_league:
            compare_to_league(cursor, player_name, stats, year)
            cursor.close()
            conn.close()
            return

        pitcher_matches, hitter_matches = find_player(cursor, player_name)

        if len(pitcher_matches) == 0 and len(hitter_matches) == 0:
            # Try fuzzy matching with accent removal
            fuzzy_pitchers, fuzzy_hitters = fuzzy_find_player(cursor, player_name)

            if not fuzzy_pitchers and not fuzzy_hitters:
                click.echo(f"Error: No players found matching '{player_name}'")
                return

            # Show fuzzy matches and ask user to pick
            all_matches = []
            if fuzzy_pitchers:
                all_matches.extend([(p, 'pitcher') for p in fuzzy_pitchers])
            if fuzzy_hitters:
                all_matches.extend([(h, 'hitter') for h in fuzzy_hitters])

            click.echo(f"No exact match found for '{player_name}'. Did you mean:")
            for idx, (name, player_type) in enumerate(all_matches, 1):
                click.echo(f"  {idx}. {name} ({player_type})")

            if len(all_matches) == 1:
                choice = click.confirm(f"\nShow stats for {all_matches[0][0]}?", default=True)
                if not choice:
                    return
                player_name = all_matches[0][0]
            else:
                try:
                    choice = click.prompt("\nEnter number (or 0 to cancel)", type=int, default=0)
                    if choice == 0 or choice < 0 or choice > len(all_matches):
                        return
                    player_name = all_matches[choice - 1][0]
                except (ValueError, click.Abort):
                    return

            # Re-search with the corrected name
            pitcher_matches, hitter_matches = find_player(cursor, player_name)

            if len(pitcher_matches) == 0 and len(hitter_matches) == 0:
                click.echo(f"Error: No players found matching '{player_name}'")
                return

        # Check for multiple unique players in EITHER category before rendering
        has_multiple = False

        # Check pitchers
        pitcher_count = 0
        unique_pitchers = set()
        unique_pitcher_ids = set()
        if pitcher_matches:
            cursor.execute(f"SELECT * FROM pitcher_stats LIMIT 1")
            column_names = [desc[0] for desc in cursor.description]
            player_col_idx = column_names.index('player')
            player_id_idx = column_names.index('player_additional')
            unique_pitchers = set(re.sub(r'[*#+]', '', match[player_col_idx]).strip() for match in pitcher_matches)
            unique_pitcher_ids = set(match[player_id_idx] for match in pitcher_matches if match[player_id_idx])
            pitcher_count = len(unique_pitchers)
            if pitcher_count > 1:
                has_multiple = True

        # Check hitters
        hitter_count = 0
        unique_hitters = set()
        unique_hitter_ids = set()
        if hitter_matches:
            cursor.execute(f"SELECT * FROM hitter_stats LIMIT 1")
            column_names = [desc[0] for desc in cursor.description]
            player_col_idx = column_names.index('player')
            player_id_idx = column_names.index('player_additional')
            unique_hitters = set(re.sub(r'[*#+]', '', match[player_col_idx]).strip() for match in hitter_matches)
            unique_hitter_ids = set(match[player_id_idx] for match in hitter_matches if match[player_id_idx])
            hitter_count = len(unique_hitters)
            if hitter_count > 1:
                has_multiple = True

        # Check if pitcher and hitter are the same person (two-way player) by comparing IDs
        same_person = bool(unique_pitcher_ids & unique_hitter_ids)  # Intersection of IDs

        # Check for exact name duplicates (same name appearing in multiple positions)
        all_names = list(unique_pitchers) + list(unique_hitters)
        name_counts = {}
        for name in all_names:
            name_counts[name] = name_counts.get(name, 0) + 1

        has_exact_duplicates = any(count > 1 for count in name_counts.values())

        # Check if we should prompt for selection
        total_unique_players = len(unique_pitchers | unique_hitters)  # Union for total unique
        if has_exact_duplicates and not same_person:
            click.echo(f"Multiple players found matching '{player_name}':")

            # Build a list of choices
            choices = []

            if pitcher_matches:
                cursor.execute(f"SELECT * FROM pitcher_stats LIMIT 1")
                column_names = [desc[0] for desc in cursor.description]
                player_col_idx = column_names.index('player')
                team_col_idx = column_names.index('team')
                year_col_idx = column_names.index('year')

                for player in sorted(unique_pitchers):
                    player_matches_filtered = [match for match in pitcher_matches if re.sub(r'[*#+]', '', match[player_col_idx]).strip() == player]
                    season_2025 = [m for m in player_matches_filtered if m[year_col_idx] == 2025]

                    if season_2025:
                        teams = [m[team_col_idx] for m in season_2025 if '2TM' not in m[team_col_idx] and '3TM' not in m[team_col_idx]]
                        if not teams:
                            teams = [season_2025[0][team_col_idx]]
                    else:
                        most_recent_year = max(m[year_col_idx] for m in player_matches_filtered)
                        recent_matches = [m for m in player_matches_filtered if m[year_col_idx] == most_recent_year]
                        teams = [m[team_col_idx] for m in recent_matches if '2TM' not in m[team_col_idx] and '3TM' not in m[team_col_idx]]
                        if not teams:
                            teams = [recent_matches[0][team_col_idx]]

                    team_str = ', '.join(teams) if len(teams) > 1 else teams[0]
                    choices.append((player, 'pitcher', team_str, player_matches_filtered))

            if hitter_matches:
                cursor.execute(f"SELECT * FROM hitter_stats LIMIT 1")
                column_names = [desc[0] for desc in cursor.description]
                player_col_idx = column_names.index('player')
                team_col_idx = column_names.index('team')
                year_col_idx = column_names.index('year')

                for player in sorted(unique_hitters):
                    player_matches_filtered = [match for match in hitter_matches if re.sub(r'[*#+]', '', match[player_col_idx]).strip() == player]
                    season_2025 = [m for m in player_matches_filtered if m[year_col_idx] == 2025]

                    if season_2025:
                        teams = [m[team_col_idx] for m in season_2025 if '2TM' not in m[team_col_idx] and '3TM' not in m[team_col_idx]]
                        if not teams:
                            teams = [season_2025[0][team_col_idx]]
                    else:
                        most_recent_year = max(m[year_col_idx] for m in player_matches_filtered)
                        recent_matches = [m for m in player_matches_filtered if m[year_col_idx] == most_recent_year]
                        teams = [m[team_col_idx] for m in recent_matches if '2TM' not in m[team_col_idx] and '3TM' not in m[team_col_idx]]
                        if not teams:
                            teams = [recent_matches[0][team_col_idx]]

                    team_str = ', '.join(teams) if len(teams) > 1 else teams[0]
                    choices.append((player, 'hitter', team_str, player_matches_filtered))

            # Display choices
            click.echo()
            for idx, (name, player_type, team, _) in enumerate(choices, 1):
                click.echo(f"  {idx}. {name} ({team}) - {player_type.upper()}")

            # Prompt user to choose
            try:
                choice = click.prompt("\nEnter number (or 0 to cancel)", type=int, default=0)
                if choice == 0 or choice < 0 or choice > len(choices):
                    return

                selected_name, selected_type, _, selected_matches = choices[choice - 1]

                # Update matches to only the selected player
                if selected_type == 'pitcher':
                    pitcher_matches = selected_matches
                    hitter_matches = []
                else:
                    hitter_matches = selected_matches
                    pitcher_matches = []

            except (ValueError, click.Abort):
                return
        elif total_unique_players > 1:
            # Multiple different names - show list without choosing
            click.echo(f"Multiple players found matching '{player_name}':")
            if pitcher_matches:
                if pitcher_count > 1:
                    click.echo("\nPITCHERS:")
                else:
                    click.echo("\nPITCHER:")
                list_matching_players(cursor, pitcher_matches, 'pitcher')
            if hitter_matches:
                if hitter_count > 1:
                    click.echo("\nHITTERS:")
                else:
                    click.echo("\nHITTER:")
                list_matching_players(cursor, hitter_matches, 'hitter')
            return

        if pitcher_matches and hitter_matches:
            # Check if this is Shohei Ohtani (only true two-way player)
            cursor.execute(f"SELECT * FROM pitcher_stats LIMIT 1")
            column_names = [desc[0] for desc in cursor.description]
            player_col_idx = column_names.index('player')
            first_pitcher = pitcher_matches[0]
            player_name_normalized = re.sub(r'[*#+]', '', first_pitcher[player_col_idx]).strip().lower()

            is_ohtani = 'shohei ohtani' in player_name_normalized

            if is_ohtani:
                # For Ohtani, show both pitching and hitting stats
                if stats:
                    show_pitcher = False
                    show_hitter = False

                    for stat in stats:
                        stat_lower = stat.lower()
                        # Normalize stat names
                        if stat_lower == 'w-l%':
                            stat_key = 'w_l_pct'
                        elif stat_lower == 'so/bb':
                            stat_key = 'so_bb'
                        elif stat_lower == 'era+':
                            stat_key = 'era_plus'
                        elif stat_lower == 'ops+':
                            stat_key = 'ops_plus'
                        elif stat_lower == 'rbat+':
                            stat_key = 'rbat_plus'
                        elif stat_lower == '2b':
                            stat_key = 'doubles'
                        elif stat_lower == '3b':
                            stat_key = 'triples'
                        elif stat_lower == 'h/9':
                            stat_key = 'h9'
                        elif stat_lower == 'hr/9':
                            stat_key = 'hr9'
                        elif stat_lower == 'bb/9':
                            stat_key = 'bb9'
                        elif stat_lower == 'so/9':
                            stat_key = 'so9'
                        else:
                            stat_key = stat_lower.replace('-', '_').replace('/', '_')

                        category = get_stat_category(stat_key)
                        if category == 'pitcher':
                            show_pitcher = True
                        elif category == 'hitter':
                            show_hitter = True
                        else:  # common stat
                            show_pitcher = True
                            show_hitter = True

                    if show_pitcher:
                        render_player(cursor, pitcher_matches, 'pitcher', stats, year)
                    if show_hitter:
                        render_player(cursor, hitter_matches, 'hitter', stats, year)
                else:
                    # No specific stats requested, show both
                    render_player(cursor, pitcher_matches, 'pitcher', stats, year)
                    render_player(cursor, hitter_matches, 'hitter', stats, year)
            else:
                # Not Ohtani - determine primary position by total games
                cursor.execute(f"SELECT * FROM pitcher_stats LIMIT 1")
                pitcher_column_names = [desc[0] for desc in cursor.description]
                pitcher_g_idx = pitcher_column_names.index('g')
                pitcher_total_games = sum(match[pitcher_g_idx] for match in pitcher_matches if match[pitcher_g_idx] is not None)

                cursor.execute(f"SELECT * FROM hitter_stats LIMIT 1")
                hitter_column_names = [desc[0] for desc in cursor.description]
                hitter_g_idx = hitter_column_names.index('g')
                hitter_total_games = sum(match[hitter_g_idx] for match in hitter_matches if match[hitter_g_idx] is not None)

                # Show whichever has more games
                if pitcher_total_games >= hitter_total_games:
                    render_player(cursor, pitcher_matches, 'pitcher', stats, year)
                else:
                    render_player(cursor, hitter_matches, 'hitter', stats, year)
        elif pitcher_matches:
            render_player(cursor, pitcher_matches, 'pitcher', stats, year)
        else:
            render_player(cursor, hitter_matches, 'hitter', stats, year)

        cursor.close()
        conn.close()

    except sqlite3.Error as e:
        click.echo(f"Database error: {e}")
    except Exception as e:
        click.echo(f"Error: {e}")

if __name__ == '__main__':
    main()
