import re
from pathlib import Path
from typing import Dict, List


LEAGUE_ARCHIVE_DIRS: Dict[str, str] = {
    "EPL": "1_Top_Five_Europe/ENG_England",
    "Championship": "1_Top_Five_Europe/ENG_England",
    "La_liga": "1_Top_Five_Europe/ESP_Spain",
    "Bundesliga": "1_Top_Five_Europe/GER_Germany",
    "Serie_A": "1_Top_Five_Europe/ITA_Italy",
    "Ligue_1": "1_Top_Five_Europe/FRA_France",
    "Eredivisie": "2_Other_Europe/NED_Netherlands",
}


TEAM_FILENAME_ALIASES: Dict[str, List[str]] = {
    "arsenal": ["Arsenal"],
    "athleticclub": ["Athletic Club", "Athletic_Bilbao"],
    "atleticomadrid": ["Atletico_Madrid", "Atletico Madrid"],
    "augsburg": ["FC_Augsburg", "Augsburg"],
    "bayerleverkusen": ["Bayer_Leverkusen", "Bayer Leverkusen"],
    "bayernmunich": ["Bayern_Munich", "Bayern Munich"],
    "bologna": ["Bologna"],
    "borussiamgladbach": ["Borussia M.Gladbach", "Borussia_Monchengladbach"],
    "crystalpalace": ["Crystal_Palace", "Crystal Palace"],
    "eintrachtfrankfurt": ["Eintracht_Frankfurt", "Eintracht Frankfurt"],
    "everton": ["Everton"],
    "fccologne": ["FC_Cologne", "FC Cologne", "Koln"],
    "fcheidenheim": ["FC_Heidenheim", "FC Heidenheim"],
    "getafe": ["Getafe"],
    "hamburgersv": ["Hamburger_SV", "Hamburger SV"],
    "hoffenheim": ["Hoffenheim"],
    "celtavigo": ["Celta_Vigo", "Celta Vigo"],
    "liverpool": ["Liverpool"],
    "mainz05": ["Mainz_05", "Mainz 05"],
    "newcastleunited": ["Newcastle_United", "Newcastle United"],
    "roma": ["AS_Roma", "Roma"],
    "southampton": ["Southampton"],
    "stpauli": ["St_Pauli", "St Pauli"],
    "tottenhamhotspur": ["Tottenham_Hotspur", "Tottenham Hotspur"],
    "tottenham": ["Tottenham_Hotspur", "Tottenham Hotspur", "Tottenham"],
    "westham": ["West_Ham_United", "West Ham"],
    "wolfsburg": ["Wolfsburg"],
    "wolverhampton": ["Wolverhampton", "Wolverhampton Wanderers"],
    "inter": ["Inter_Milan", "Inter"],
    "leeds": ["Leeds_United", "Leeds"],
    "manchestercity": ["Manchester_City", "Manchester City"],
    "parissaintgermain": ["PSG", "Paris Saint Germain", "Paris_Saint_Germain"],
    "rayovallecano": ["Rayo_Vallecano", "Rayo Vallecano"],
    "realbetis": ["Real_Betis", "Real Betis"],
    "realmadrid": ["Real_Madrid", "Real Madrid"],
    "realoviedo": ["Real_Oviedo", "Real Oviedo"],
    "realsociedad": ["Real_Sociedad", "Real Sociedad"],
    "vfbstuttgart": ["VfB_Stuttgart", "VfB Stuttgart"],
}


def normalize_team_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def candidate_team_filenames(team_name: str) -> List[str]:
    raw = str(team_name).strip()
    key = normalize_team_key(raw)
    candidates = TEAM_FILENAME_ALIASES.get(key, [])
    candidates = candidates + [raw.replace(" ", "_"), raw, raw.replace("_", " ")]
    ordered: List[str] = []
    for candidate in candidates:
        txt = str(candidate).strip()
        if txt and txt not in ordered:
            ordered.append(txt)
    return ordered


def canonical_team_filename(team_name: str) -> str:
    raw = str(team_name).strip()
    key = normalize_team_key(raw)
    aliases = TEAM_FILENAME_ALIASES.get(key) or []
    if aliases:
        return str(aliases[0]).strip()
    return raw.replace(" ", "_")


def league_archive_dir(root: Path, league: str) -> Path:
    relative = LEAGUE_ARCHIVE_DIRS.get(str(league).strip())
    if relative:
        return root / relative
    return root / str(league).strip()
