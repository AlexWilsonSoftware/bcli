# Baseball CLI (bcli)

A command-line tool for querying MLB player statistics

## Setup

### 1. Clone the Repository

```bash
git clone <repo-url>
cd bcli
```

The database (`baseball_stats.db`) is already included with 2022-2025 stats!

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Install CLI (Optional)

To use `bcli` instead of `python3 bcli.py`:

```bash
pip install -e .
```

## Usage

### Basic Player Lookup

```bash
# View all career stats
bcli l.webb
bcli bryan.woo
bcli roki.s

# View specific year
bcli l.webb -y 24
bcli bryan.woo -y 2023

# Bold season totals indicate player led league
# Italic season totals indicate player led both leagues
```

### Filter Specific Stats

```bash
# Pitchers
bcli l.webb -s era -s whip -s fip

# Hitters
bcli ohtani -s hr -s rbi -s ops
```

### Compare Two Players

```bash
# Compare head-to-head (defaults to 2025)
bcli judge -c ohtani

# Compare specific year
bcli l.webb -y 24 -c verlander

# Compare specific stats
bcli judge -c ohtani -s hr -s ops -s war
```

### Compare to Team Average

```bash
# Compare to team average (all years)
bcli l.webb -ct

# Specific year
bcli judge -y 24 -ct

# Shows only rate stats (ERA, WHIP, BA, OBP, etc.)
# Green = above average, Orange = below average
```

### Compare to League Average

```bash
# Compare to league average
bcli ohtani -cl

# Specific year
bcli l.webb -y 23 -cl
```

## Troubleshooting

### Multiple Players Found

If you get a "Multiple players found" error:

```bash
$ bcli williams

Error: Multiple hitters found matching 'williams':
  - Luke Williams (ATL)
  - Nick Williams (DET)
```

Be more specific:

```bash
$ bcli l.williams
```

### Comparison Restrictions

- Cannot use `-c`, `-ct`, and `-cl` together (choose one comparison mode)
- Cannot compare players of different types (pitcher vs hitter)
- Cannot compare traded players (2TM/3TM) to team average (use `-cl` for league instead)

## Data Sources

Stats are from Baseball Reference for the 2022-2025 seasons. The database includes:
- 4 years of player stats (pitchers and hitters)
- 4 years of team aggregates
- League averages for each year

## Contributing

To add more years of data:

1. Export CSV from Baseball Reference
2. Run: `python3 load_data.py your_file.csv YEAR`
3. For team stats: Add CSVs and run `python3 load_team_stats.py`

## License

MIT
