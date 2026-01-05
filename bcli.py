#!/usr/bin/env python3
import click
import sqlite3
import os
import re

DB_PATH = os.path.join(os.path.dirname(__file__), 'baseball_stats.db')

def get_db_connection():
    return sqlite3.connect(DB_PATH)

def parse_name_query(name_query):
    if '.' not in name_query:
        return None, name_query

    parts = name_query.split('.', 1)
    first_pattern = parts[0]
    last_pattern = parts[1]

    return first_pattern, last_pattern

def find_player(cursor, name_query):
    first_pattern, last_pattern = parse_name_query(name_query)

    if first_pattern is None:
        pattern = f'%{last_pattern.lower()}%'
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

    return pitcher_matches, hitter_matches

def format_stat_value(value):
    if value is None:
        return 'N/A'
    if isinstance(value, (int, float)):
        return str(value)
    return value

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

def render_player(cursor, matches, player_type, stats, year):
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

    first_player = dict(zip(column_names, matches[0]))
    player_name = first_player['player']
    click.echo(f"\n{first_player['player']} ({player_type.upper()})")
    click.echo("=" * 50)

    column_names = [desc[0] for desc in cursor.description]

    if len(matches) > 1:
        player_col_idx = get_column_index(cursor, 'player')
        unique_players = set(match[player_col_idx] for match in matches)

        if len(unique_players) > 1:
            click.echo(f"Error: Multiple players found matching '{player_name}':")
            team_col_idx = get_column_index(cursor, 'team')
            year_col_idx = get_column_index(cursor, 'year')

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
        col_widths.append(max_width)

    def calculate_yearly_league_leaders():
        leaders_by_year = {}

        if player_type == 'pitcher':
            lower_is_better = {'era'}
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

    header_parts = []
    for idx, (label, key) in enumerate(all_stats):
        header_parts.append(label.ljust(col_widths[idx]))
    click.echo("  ".join(header_parts))

    def get_stat_formatting(year, player_league, player_data, stat_key):
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
            lower_is_better_check = {'era', 'whip', 'fip', 'h9', 'hr9', 'bb9'}
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
        last_historical_year = historical_data[-1].get('year')
        if last_historical_year and last_historical_year < 2024:
            missing_years = list(range(last_historical_year + 1, 2025))
            gap_msg = f"[Did not play in {', '.join(map(str, missing_years))}]"
            click.echo(gap_msg)

    if season_2025_rows and historical_rows:
        click.echo()

    if season_2025_rows:
        for row_idx, row in enumerate(season_2025_rows):
            print_row(row, season_2025_data[row_idx])

    click.echo()

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

    # Define which stats are "lower is better"
    lower_is_better = {'era', 'whip', 'fip', 'h9', 'hr9', 'bb9', 'so', 'gidp', 'cs'}

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

@click.command()
@click.argument('player_name')
@click.option('-s', '--stats', multiple=True, help='Specific stats to display (e.g., war era)')
@click.option('-y', '--year', help='Filter by year (e.g., 2022 or 22)')
@click.option('-c', '--compare', help='Compare with another player')
def main(player_name, stats, year, compare):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # If compare flag is set, use comparison mode
        if compare:
            compare_players(cursor, player_name, compare, stats, year)
            cursor.close()
            conn.close()
            return

        pitcher_matches, hitter_matches = find_player(cursor, player_name)

        if len(pitcher_matches) == 0 and len(hitter_matches) == 0:
            click.echo(f"Error: No players found matching '{player_name}'")
            return

        if pitcher_matches and hitter_matches:
            # For two-way players, determine which stats to show based on requested stats
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
