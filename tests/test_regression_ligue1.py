import json, unittest
from pathlib import Path
from engine.postmatch_governance.evidence_builder import PostmatchEvidenceBuilder

BASE = Path('/Users/liumingwei/vaults/AresVault/03_Match_Audits/2026-2027/Ligue1/GW06/postmatch')

class TestLigue1GW06PostmatchRegression(unittest.TestCase):

    def test_t1_nice_strasbourg_halftime_and_owngoal(self):
        """T1: 45+2' 乌龙球计入上半场比分，且 actual_scorer_team 与 benefiting_team 严格解耦"""
        p = BASE / '2995358' / '00-fotmob-raw.json'
        with open(p) as f:
            raw = json.load(f)
        ext = PostmatchEvidenceBuilder.extract_stats_from_raw(raw, 9831, 9848)
        
        self.assertEqual(ext['final_score'], '2-1')
        self.assertEqual(ext['halftime_score'], '1-1', "45+2' 乌龙球必须计入半场比分 (1-1)")
        
        og = [g for g in ext['goals'] if g['is_own_goal']][0]
        self.assertEqual(og['player'], 'Pape Demba Diop')
        self.assertEqual(og['time_str'], '45 + 2')
        self.assertEqual(og['actual_scorer_team'], 'away', 'Pape Demba Diop 属于客队 Strasbourg')
        self.assertEqual(og['benefiting_team'], 'home', '获益方为主队 Nice')
        self.assertEqual(og['goal_type'], 'OWN_GOAL')
        self.assertEqual(og['score_after'], '1-1')

    def test_t2_rennes_auxerre_added_time_goals_not_merged(self):
        """T2: 90+2' 与 90+4' 不得合并，保留独立补时记录"""
        p = BASE / '2995360' / '00-fotmob-raw.json'
        with open(p) as f:
            raw = json.load(f)
        ext = PostmatchEvidenceBuilder.extract_stats_from_raw(raw, raw['general']['homeTeam']['id'], raw['general']['awayTeam']['id'])
        
        dia_goals = [g for g in ext['goals'] if g['player'] == 'Boulaye Dia']
        self.assertEqual(len(dia_goals), 2, "Boulaye Dia 应有两粒独立补时进球")
        self.assertEqual(dia_goals[0]['time_str'], '90 + 2')
        self.assertEqual(dia_goals[0]['overload_time'], 2)
        self.assertEqual(dia_goals[0]['score_after'], '3-1')
        self.assertEqual(dia_goals[1]['time_str'], '90 + 4')
        self.assertEqual(dia_goals[1]['overload_time'], 4)
        self.assertEqual(dia_goals[1]['score_after'], '4-1')

    def test_t3_owngoal_scorer_benefiting_score_accounting(self):
        """T3: Own-goal actual_scorer_team 与 benefiting_team 不一致时仍正确计分"""
        p = BASE / '2995358' / '00-fotmob-raw.json'
        with open(p) as f:
            raw = json.load(f)
        ext = PostmatchEvidenceBuilder.extract_stats_from_raw(raw, 9831, 9848)
        
        goals = ext['goals']
        self.assertEqual(len(goals), 3)
        self.assertEqual(goals[0]['score_after'], '0-1') # Essugo 41'
        self.assertEqual(goals[1]['score_after'], '1-1') # Diop OG 45+2' -> 主队比分增加到 1
        self.assertEqual(goals[2]['score_after'], '2-1') # Clauss 81' -> 主队比分增加到 2

    def test_troyes_marseille_penalty_added_time(self):
        """Troyes vs Marseille: 90+8' 点球进球保留 timeStr 与 overloadTime"""
        p = BASE / '2995361' / '00-fotmob-raw.json'
        with open(p) as f:
            raw = json.load(f)
        ext = PostmatchEvidenceBuilder.extract_stats_from_raw(raw, raw['general']['homeTeam']['id'], raw['general']['awayTeam']['id'])
        
        abdelli_goal = [g for g in ext['goals'] if g['player'] == 'Himad Abdelli'][0]
        self.assertEqual(abdelli_goal['time_str'], '90 + 8')
        self.assertEqual(abdelli_goal['overload_time'], 8)
        self.assertEqual(abdelli_goal['goal_type'], 'PENALTY')
        self.assertEqual(abdelli_goal['score_after'], '0-2')

    def test_psg_le_mans_penalties_and_var(self):
        """PSG vs Le Mans: 区分 19' 有效点球进球、90+3' MissedPenalty 与 90+3' VAR 取消点球"""
        p = BASE / '2995359' / '00-fotmob-raw.json'
        with open(p) as f:
            raw = json.load(f)
        ext = PostmatchEvidenceBuilder.extract_stats_from_raw(raw, raw['general']['homeTeam']['id'], raw['general']['awayTeam']['id'])
        
        pen_goals = [g for g in ext['goals'] if g['is_penalty']]
        self.assertEqual(len(pen_goals), 1, "仅有 1 粒有效点球进球")
        self.assertEqual(pen_goals[0]['player'], 'Lucas Beraldo')
        self.assertEqual(pen_goals[0]['time_str'], '19')
        
        self.assertEqual(len(ext['missed_penalties']), 1)
        self.assertEqual(ext['missed_penalties'][0]['player'], 'Désiré Doué')
        self.assertEqual(ext['missed_penalties'][0]['time_str'], '90 + 3')
        
        self.assertEqual(len(ext['var_events']), 1)
        self.assertEqual(ext['var_events'][0]['player'], 'Vitinha')
        self.assertIn('Penalty cancelled', ext['var_events'][0]['values'])

    def test_brest_angers_special_events(self):
        """Brest vs Angers: 30' Lala 点球、58' VAR 黄牌取消、59' Lefort 红牌"""
        p = BASE / '2995353' / '00-fotmob-raw.json'
        with open(p) as f:
            raw = json.load(f)
        ext = PostmatchEvidenceBuilder.extract_stats_from_raw(raw, raw['general']['homeTeam']['id'], raw['general']['awayTeam']['id'])
        
        lala_pen = [g for g in ext['goals'] if g['player'] == 'Kenny Lala'][0]
        self.assertTrue(lala_pen['is_penalty'])
        self.assertEqual(lala_pen['time_str'], '30')
        
        self.assertEqual(len(ext['var_events']), 1)
        self.assertEqual(ext['var_events'][0]['player'], 'Jordan Lefort')
        self.assertIn('Yellow card cancelled', ext['var_events'][0]['values'])
        
        self.assertEqual(len(ext['red_cards']), 1)
        self.assertEqual(ext['red_cards'][0]['player'], 'Jordan Lefort')
        self.assertEqual(ext['red_cards'][0]['time_str'], '59')

    def test_monaco_toulouse_penalty(self):
        """Monaco vs Toulouse: 48' Tengstedt 点球进球"""
        p = BASE / '2995357' / '00-fotmob-raw.json'
        with open(p) as f:
            raw = json.load(f)
        ext = PostmatchEvidenceBuilder.extract_stats_from_raw(raw, raw['general']['homeTeam']['id'], raw['general']['awayTeam']['id'])
        
        tengstedt_pen = [g for g in ext['goals'] if g['player'] == 'Casper Tengstedt'][0]
        self.assertTrue(tengstedt_pen['is_penalty'])
        self.assertEqual(tengstedt_pen['time_str'], '48')

if __name__ == '__main__':
    unittest.main()
