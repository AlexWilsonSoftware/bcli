# Baseball CLI (bcli)

A command-line tool for querying pitcher statistics using SQLite.

## Setup

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Up Database

Create the SQLite database and table:

```bash
python3 db_setup.py
```

This creates a `baseball_stats.db` file in your project directory.

### 3. Load Data

Place your CSV file in the project directory and load it:

```bash
python3 load_data.py pitcher_stats_2025.csv
```

Replace `pitcher_stats.csv` with the path to your CSV file.

### 4. Make CLI Executable (Optional)

```bash
chmod +x bcli.py
```

## Usage

### Query a player by name

```bash
# Using first initial and last name
python3 bcli.py l.webb

# Using full name
python3 bcli.py logan.webb
```

### Query specific stats

```bash
# Get WAR and ERA
python3 bcli.py l.webb -s war -s era

# Get multiple stats
python3 bcli.py l.webb -s war -s era -s whip -s fip
```

### Available Stats

All CSV columns are available, including:
- WAR, W, L, W-L%, ERA
- G, GS, GF, CG, SHO, SV
- IP, H, R, ER, HR
- BB, IBB, SO, HBP, BK, WP
- BF, ERA+, FIP, WHIP
- H9, HR9, BB9, SO9, SO/BB

## Name Matching

The CLI supports flexible name matching:

- `l.webb` - Matches players with first name starting with "l" and last name containing "webb"
- `logan.webb` - Matches players with first name starting with "logan" and last name containing "webb"

If multiple players match, the CLI will show all matches and ask you to be more specific.

## Example

```bash
$ python3 bcli.py l.webb

Logan Webb - SFG
==================================================
WAR: 3.8
W-L: 15-11
ERA: 3.22
IP: 207.0
SO: 224
WHIP: 1.237
FIP: 2.60

$ python3 bcli.py l.webb -s war -s era -s fip

Logan Webb - SFG
==================================================
WAR: 3.8
ERA: 3.22
FIP: 2.60
```

## CSV Format

Your CSV should have the following columns:
```
Rk,Player,Age,Team,Lg,WAR,W,L,W-L%,ERA,G,GS,GF,CG,SHO,SV,IP,H,R,ER,HR,BB,IBB,SO,HBP,BK,WP,BF,ERA+,FIP,WHIP,H9,HR9,BB9,SO9,SO/BB,Awards,Player-additional
```

## Distribution

This CLI is portable! To share with others:
1. Package the entire directory including `baseball_stats.db`
2. Recipients only need Python 3 and to run `pip install -r requirements.txt`
3. No database server setup required - SQLite runs locally
