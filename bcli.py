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
        cursor.execute('''
            SELECT * FROM pitcher_stats
            WHERE LOWER(player) LIKE ?
            ORDER BY year ASC, team
        ''', (f'%{last_pattern.lower()}%',))
    else:
        cursor.execute('''
            SELECT * FROM pitcher_stats
            WHERE LOWER(player) LIKE ?
            ORDER BY year ASC, team
        ''', (f'{first_pattern.lower()}%{last_pattern.lower()}%',))

    return cursor.fetchall()

def format_stat_value(value):
    if value is None:
        return 'N/A'
    if isinstance(value, (int, float)):
        return str(value)
    return value

def parse_awards(awards_str):
    """Parse awards string into readable format"""
    if not awards_str:
        return None

    awards = []
    remaining = awards_str

    # Award patterns with their full names
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

@click.command()
@click.argument('player_name')
@click.option('-s', '--stats', multiple=True, help='Specific stats to display (e.g., war era)')
@click.option('-y', '--year', help='Filter by year (e.g., 2022 or 22)')
def main(player_name, stats, year):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        matches = find_player(cursor, player_name)

        if len(matches) == 0:
            click.echo(f"Error: No players found matching '{player_name}'")
            return

        column_names = [desc[0] for desc in cursor.description]

        # Filter by year if specified
        if year:
            # Parse year input (handle both "2022" and "22" formats)
            if len(year) == 2 and year.isdigit():
                year_filter = int(f"20{year}")
            elif len(year) == 4 and year.isdigit():
                year_filter = int(year)
            else:
                click.echo(f"Error: Invalid year format '{year}'. Use 2022 or 22.")
                return

            year_col_idx = get_column_index(cursor, 'year')
            if year_col_idx is not None:
                matches = [m for m in matches if m[year_col_idx] == year_filter]

            if len(matches) == 0:
                click.echo(f"Error: No data found for '{player_name}' in {year_filter}")
                return

        if len(matches) > 1:
            player_col_idx = get_column_index(cursor, 'player')
            unique_players = set(match[player_col_idx] for match in matches)

            if len(unique_players) > 1:
                click.echo(f"Error: Multiple players found matching '{player_name}':")
                team_col_idx = get_column_index(cursor, 'team')
                year_col_idx = get_column_index(cursor, 'year')

                for player in sorted(unique_players):
                    player_matches = [match for match in matches if match[player_col_idx] == player]

                    # Find current team(s) (2025 first, otherwise most recent year)
                    season_2025 = [m for m in player_matches if m[year_col_idx] == 2025]

                    if season_2025:
                        # Get all 2025 teams (excluding 2TM/3TM)
                        teams = [m[team_col_idx] for m in season_2025 if '2TM' not in m[team_col_idx] and '3TM' not in m[team_col_idx]]
                        if not teams:
                            teams = [season_2025[0][team_col_idx]]
                    else:
                        # Get most recent year's team(s)
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

        # Get player name from first match
        first_player = dict(zip(column_names, matches[0]))
        player_name = first_player['player']

        click.echo(f"{player_name}")
        click.echo("=" * 50)

        if stats:
            # When filtering specific stats, show them for each season
            for idx, player_data in enumerate(matches):
                player_dict = dict(zip(column_names, player_data))
                year = player_dict.get('year', 'N/A')
                team = player_dict.get('team', 'N/A')
                click.echo(f"\n{year} - {team}")
                for stat in stats:
                    stat_lower = stat.lower()
                    if stat_lower == 'w-l%':
                        stat_key = 'w_l_pct'
                    elif stat_lower == 'so/bb':
                        stat_key = 'so_bb'
                    elif stat_lower == 'era+':
                        stat_key = 'era_plus'
                    else:
                        stat_key = stat_lower.replace('-', '_').replace('/', '_')

                    if stat_key in player_dict:
                        value = format_stat_value(player_dict[stat_key])
                        click.echo(f"{stat.upper()}: {value}")
                    else:
                        click.echo(f"{stat.upper()}: Unknown stat")
        else:
            # Display all stats as table with all seasons
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

            # First pass: collect all data and calculate column widths
            # Separate 2025 season from historical seasons
            season_2025_rows = []
            season_2025_data = []
            historical_rows = []
            historical_data = []

            prev_year = None
            gap_rows = []  # Track which rows should have gap messages before them (for historical only)

            for idx, player_data in enumerate(matches):
                player_dict = dict(zip(column_names, player_data))
                current_year = player_dict.get('year')

                # Build value row
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

                # Separate 2025 from other years
                if current_year == 2025:
                    season_2025_rows.append(row_values)
                    season_2025_data.append(player_dict)
                else:
                    # Check for gap years in historical data
                    if len(historical_rows) > 0 and prev_year is not None and current_year is not None:
                        year_gap = current_year - prev_year
                        if year_gap > 1:
                            missing_years = list(range(prev_year + 1, current_year))
                            gap_msg = f"[Did not play in {', '.join(map(str, missing_years))}]"
                            gap_rows.append((len(historical_rows), gap_msg))

                    historical_rows.append(row_values)
                    historical_data.append(player_dict)
                    prev_year = current_year

            # Combine for column width calculation
            all_rows = season_2025_rows + historical_rows
            all_row_data = season_2025_data + historical_data

            # Detect traded years with and without All-Star
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

            # Calculate column widths
            col_widths = []
            for col_idx, (label, key) in enumerate(all_stats):
                max_width = len(label)
                for row in all_rows:
                    max_width = max(max_width, len(row[col_idx]))
                col_widths.append(max_width)

            # Calculate league leaders for each year and stat
            def calculate_yearly_league_leaders():
                """Calculate who leads in each stat for NL and AL for each year"""
                leaders_by_year = {}

                # Stats where lower is better
                lower_is_better = {'era', 'whip', 'fip', 'h9', 'hr9', 'bb9'}

                # Rate stats that require minimum IP qualification (162 IP for full season)
                rate_stats_needing_qualification = {'era', 'whip', 'fip', 'h9', 'hr9', 'bb9', 'so9', 'w_l_pct', 'era_plus', 'so_bb'}

                # Get years from player's data
                years = set(row.get('year') for row in all_row_data if row.get('year'))

                for year in years:
                    leaders_by_year[year] = {}

                    # Query ALL pitchers for this year from database to find actual leaders
                    cursor.execute('''
                        SELECT * FROM pitcher_stats
                        WHERE year = ?
                    ''', (year,))
                    all_pitchers_year = cursor.fetchall()
                    year_data = [dict(zip(column_names, row)) for row in all_pitchers_year]

                    for label, key in all_stats:
                        if key in ['year', 'age', 'team', 'lg', 'awards']:
                            continue

                        leaders_by_year[year][key] = {'NL': None, 'AL': None}

                        for league in ['NL', 'AL']:
                            league_data = [row for row in year_data if row.get('lg') == league]
                            if not league_data:
                                continue

                            # Get all valid values for this stat in this league
                            stat_values = []
                            for row in league_data:
                                val = row.get(key)
                                ip = row.get('ip')

                                if val is not None and val != '':
                                    try:
                                        # Apply IP qualification for rate stats (1 IP per team game)
                                        # Use 162 for 2022-2024, proportional for 2025 based on date
                                        if key in rate_stats_needing_qualification:
                                            if ip is None:
                                                continue
                                            ip_float = float(ip)
                                            # Require 162 IP for qualification
                                            if ip_float < 162:
                                                continue

                                        stat_values.append((float(val), row))
                                    except (ValueError, TypeError):
                                        pass

                            if not stat_values:
                                continue

                            # Find leader
                            if key in lower_is_better:
                                leaders_by_year[year][key][league] = min(stat_values, key=lambda x: x[0])[1]
                            else:
                                leaders_by_year[year][key][league] = max(stat_values, key=lambda x: x[0])[1]

                return leaders_by_year

            # Calculate leaders for all years
            league_leaders = calculate_yearly_league_leaders()

            # Print headers
            header_parts = []
            for idx, (label, key) in enumerate(all_stats):
                header_parts.append(label.ljust(col_widths[idx]))
            click.echo("  ".join(header_parts))

            # Helper function to check if player leads league(s) in a stat
            def get_stat_formatting(year, player_league, player_data, stat_key):
                """Returns ANSI codes for bold/italic if player leads league(s)"""
                if year not in league_leaders or stat_key not in league_leaders[year]:
                    return '', ''

                leaders = league_leaders[year][stat_key]
                nl_leader = leaders.get('NL')
                al_leader = leaders.get('AL')

                # Check if this player leads their league
                leads_own_league = False
                if player_league == 'NL' and nl_leader:
                    leads_own_league = nl_leader.get('player') == player_data.get('player')
                elif player_league == 'AL' and al_leader:
                    leads_own_league = al_leader.get('player') == player_data.get('player')

                if not leads_own_league:
                    return '', ''

                # Check if they also lead MLB (have better value than other league's leader)
                lower_is_better = {'era', 'whip', 'fip', 'h9', 'hr9', 'bb9'}
                leads_mlb = False

                try:
                    player_val = float(player_data.get(stat_key))
                    other_league = 'AL' if player_league == 'NL' else 'NL'
                    other_leader = leaders.get(other_league)

                    if other_leader:
                        other_val = float(other_leader.get(stat_key))
                        if stat_key in lower_is_better:
                            leads_mlb = player_val < other_val
                        else:
                            leads_mlb = player_val > other_val
                    else:
                        # Other league has no leader, so they lead MLB by default
                        leads_mlb = True
                except (ValueError, TypeError):
                    pass

                if leads_mlb:
                    # Leads MLB (bold + italic)
                    return '\x1b[1m\x1b[3m', '\x1b[0m'
                else:
                    # Leads only their league (bold only)
                    return '\x1b[1m', '\x1b[0m'

            # Helper function to print a row with color
            def print_row(row, row_data):
                year = row_data.get('year')
                team = row_data.get('team', '')
                awards = row_data.get('awards', '')
                player_league = row_data.get('lg', '')

                # Determine season color
                season_color = None
                is_award_winner = False
                if awards:
                    # Match MVP-1, CYA-1, ROY-1 but not MVP-10, CYA-11, etc.
                    # Use lookahead to handle concatenated awards (e.g., "CYA-1MVP-13")
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

                    # Pad the value first
                    padded_value = value.ljust(col_widths[col_idx])

                    # Apply color only to the Season column
                    if key == 'year' and season_color:
                        color_code = color_codes.get(season_color, '')
                        formatted_value = f"{color_code}{padded_value}\x1b[0m"
                    else:
                        # Apply bold/italic formatting for league leaders
                        prefix, suffix = get_stat_formatting(year, player_league, row_data, key)
                        formatted_value = f"{prefix}{padded_value}{suffix}"

                    row_parts.append(formatted_value)

                line = "  ".join(row_parts)
                click.echo(line)

            # Print historical seasons first
            gap_dict = {idx: msg for idx, msg in gap_rows}
            for row_idx, row in enumerate(historical_rows):
                if row_idx in gap_dict:
                    click.echo(gap_dict[row_idx])
                print_row(row, historical_data[row_idx])

            # Check for gap between last historical year and 2025
            if season_2025_rows and historical_rows:
                last_historical_year = historical_data[-1].get('year')
                if last_historical_year and last_historical_year < 2024:
                    missing_years = list(range(last_historical_year + 1, 2025))
                    gap_msg = f"[Did not play in {', '.join(map(str, missing_years))}]"
                    click.echo(gap_msg)

            # Print separator if there are both historical and 2025 data
            if season_2025_rows and historical_rows:
                click.echo()

            # Print 2025 season last
            if season_2025_rows:
                for row_idx, row in enumerate(season_2025_rows):
                    print_row(row, season_2025_data[row_idx])

        click.echo()

        cursor.close()
        conn.close()

    except sqlite3.Error as e:
        click.echo(f"Database error: {e}")
    except Exception as e:
        click.echo(f"Error: {e}")

if __name__ == '__main__':
    main()
