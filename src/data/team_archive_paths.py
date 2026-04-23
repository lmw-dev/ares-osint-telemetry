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
    "athleticclub": ["Athletic_Bilbao", "Athletic Club"],
    "atleticomadrid": ["Atletico_Madrid", "Atletico Madrid"],
    "bayerleverkusen": ["Bayer_Leverkusen", "Bayer Leverkusen"],
    "bayernmunich": ["Bayern_Munich", "Bayern Munich"],
    "celtavigo": ["Celta_Vigo", "Celta Vigo"],
    "crystalpalace": ["Crystal_Palace", "Crystal Palace"],
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
    "westham": ["West_Ham_United", "West Ham"],
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


def league_archive_dir(root: Path, league: str) -> Path:
    relative = LEAGUE_ARCHIVE_DIRS.get(str(league).strip())
    if relative:
        return root / relative
    return root / str(league).strip()
