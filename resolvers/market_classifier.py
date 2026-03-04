import re
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class MarketClassification:
    """Represents a classified market with all extracted information"""
    category: str  # 'player_stat', 'scorer', 'game_total', 'team_win', 'period', 'player_combo', 'player_special'
    subcategory: str  # Specific market type
    clean_player: str
    clean_team: str
    direction: Optional[str]  # 'over', 'under', None
    line_value: Optional[float]
    period: Optional[str]  # '1st quarter', '1st half', '1st period', etc.
    stat_type: Optional[str]  # 'points', 'yards', 'tds', 'goals', etc.
    original_player: str
    original_market: str
    confidence: float = 0.5  # Confidence score for prediction reliability


class MarketClassifier:
    """Classifies all market types including your 157+ markets"""

    def __init__(self):
        # Confidence scores for different market types
        self.CONFIDENCE_SCORES = {
            'player_stat_ou': 0.7,  # Player stat O/U - usually reliable
            'player_stat': 0.6,  # Regular player stat
            'scorer': 0.4,  # Scorer props - less reliable
            'game': 0.8,  # Game outcomes - most reliable
            'player_prop': 0.5,  # Generic player prop - moderate
            'player_combo': 0.7,  # Combo props - usually reliable
            'player_special': 0.6,  # Special props (double double, etc.)
            'period_market': 0.8,  # Period markets - reliable
            'team_market': 0.7,  # Team markets - reliable
            'first_last_scorer': 0.4,  # First/last scorer - less reliable
        }

        # ==============================================
        # COMPLETE MARKET PATTERNS FROM YOUR LIST
        # ==============================================

        # Quarter markets
        self.QUARTER_MARKETS = {
            '1st_quarter_moneyline': [r'1st.*quarter.*moneyline', r'first.*quarter.*moneyline'],
            '1st_quarter_moneyline_3way': [r'1st.*quarter.*moneyline.*3.*way', r'first.*quarter.*moneyline.*3way'],
            '1st_quarter_spread': [r'1st.*quarter.*point.*spread', r'first.*quarter.*spread'],
            '1st_quarter_total': [r'1st.*quarter.*total.*points', r'first.*quarter.*total'],
            '1st_quarter_total_odd_even': [r'1st.*quarter.*total.*odd.*even', r'first.*quarter.*odd.*even'],
            '1st_quarter_team_to_score_last': [r'1st.*quarter.*team.*to.*score.*last',
                                               r'first.*quarter.*team.*score.*last'],

            '2nd_quarter_moneyline': [r'2nd.*quarter.*moneyline', r'second.*quarter.*moneyline'],
            '2nd_quarter_moneyline_3way': [r'2nd.*quarter.*moneyline.*3.*way', r'second.*quarter.*moneyline.*3way'],
            '2nd_quarter_spread': [r'2nd.*quarter.*point.*spread', r'second.*quarter.*spread'],
            '2nd_quarter_total': [r'2nd.*quarter.*total.*points', r'second.*quarter.*total'],
            '2nd_quarter_total_odd_even': [r'2nd.*quarter.*total.*odd.*even', r'second.*quarter.*odd.*even'],
            '2nd_quarter_team_to_score_last': [r'2nd.*quarter.*team.*to.*score.*last',
                                               r'second.*quarter.*team.*score.*last'],

            '3rd_quarter_moneyline': [r'3rd.*quarter.*moneyline', r'third.*quarter.*moneyline'],
            '3rd_quarter_moneyline_3way': [r'3rd.*quarter.*moneyline.*3.*way', r'third.*quarter.*moneyline.*3way'],
            '3rd_quarter_spread': [r'3rd.*quarter.*point.*spread', r'third.*quarter.*spread'],
            '3rd_quarter_total': [r'3rd.*quarter.*total.*points', r'third.*quarter.*total'],
            '3rd_quarter_total_odd_even': [r'3rd.*quarter.*total.*odd.*even', r'third.*quarter.*odd.*even'],

            '4th_quarter_moneyline': [r'4th.*quarter.*moneyline', r'fourth.*quarter.*moneyline'],
            '4th_quarter_moneyline_3way': [r'4th.*quarter.*moneyline.*3.*way', r'fourth.*quarter.*moneyline.*3way'],
            '4th_quarter_spread': [r'4th.*quarter.*point.*spread', r'fourth.*quarter.*spread'],
            '4th_quarter_total': [r'4th.*quarter.*total.*points', r'fourth.*quarter.*total'],
        }

        # Half markets
        self.HALF_MARKETS = {
            '1st_half_moneyline': [r'1st.*half.*moneyline', r'first.*half.*moneyline'],
            '1st_half_moneyline_3way': [r'1st.*half.*moneyline.*3.*way', r'first.*half.*moneyline.*3way'],
            '1st_half_spread': [r'1st.*half.*point.*spread', r'first.*half.*spread'],
            '1st_half_total': [r'1st.*half.*total.*points', r'first.*half.*total'],
            '1st_half_total_odd_even': [r'1st.*half.*total.*odd.*even', r'first.*half.*odd.*even'],

            '2nd_half_moneyline': [r'2nd.*half.*moneyline', r'second.*half.*moneyline'],
            '2nd_half_moneyline_3way': [r'2nd.*half.*moneyline.*3.*way', r'second.*half.*moneyline.*3way'],
            '2nd_half_spread': [r'2nd.*half.*point.*spread', r'second.*half.*spread'],
            '2nd_half_total': [r'2nd.*half.*total.*points', r'second.*half.*total'],
            '2nd_half_total_odd_even': [r'2nd.*half.*total.*odd.*even', r'second.*half.*odd.*even'],

            '2nd_half_excluding_ot_moneyline_3way': [r'2nd.*half.*excluding.*overtime.*moneyline.*3.*way'],
            '2nd_half_excluding_ot_moneyline': [r'2nd.*half.*excluding.*overtime.*moneyline'],
        }

        # 1st 3 Quarters markets
        self.FIRST_3_QUARTERS_MARKETS = {
            '1st_3_quarters_moneyline_3way': [r'1st.*3.*quarters.*moneyline.*3.*way',
                                              r'first.*3.*quarters.*moneyline.*3way'],
            '1st_3_quarters_spread': [r'1st.*3.*quarters.*point.*spread', r'first.*3.*quarters.*spread'],
            '1st_3_quarters_total': [r'1st.*3.*quarters.*total.*points', r'first.*3.*quarters.*total'],
        }

        # 4th Quarter excluding/including OT markets
        self.FOURTH_QUARTER_MARKETS = {
            '4th_quarter_excluding_ot_moneyline': [r'4th.*quarter.*excluding.*overtime.*moneyline'],
            '4th_quarter_excluding_ot_moneyline_3way': [r'4th.*quarter.*excluding.*overtime.*moneyline.*3.*way'],
            '4th_quarter_excluding_ot_spread': [r'4th.*quarter.*excluding.*overtime.*point.*spread'],
            '4th_quarter_excluding_ot_total': [r'4th.*quarter.*excluding.*overtime.*total.*points'],
            '4th_quarter_including_ot_moneyline': [r'4th.*quarter.*including.*overtime.*moneyline'],
            '4th_quarter_including_ot_moneyline_3way': [r'4th.*quarter.*including.*overtime.*moneyline.*3.*way'],
            '4th_quarter_including_ot_spread': [r'4th.*quarter.*including.*overtime.*point.*spread'],
            '4th_quarter_including_ot_total': [r'4th.*quarter.*including.*overtime.*total.*points'],
            '4th_quarter_including_ot_total_odd_even': [r'4th.*quarter.*including.*overtime.*total.*odd.*even'],
        }

        # Regulation markets
        self.REGULATION_MARKETS = {
            'regulation_moneyline_3way': [r'regulation.*moneyline.*3.*way', r'regulation.*3way'],
        }

        # Player period stats
        self.PLAYER_PERIOD_STATS = {
            'player_1st_quarter_points': [r'player.*1st.*quarter.*points', r'player.*first.*quarter.*points'],
            'player_1st_quarter_assists': [r'player.*1st.*quarter.*assists', r'player.*first.*quarter.*assists'],
            'player_1st_quarter_rebounds': [r'player.*1st.*quarter.*rebounds', r'player.*first.*quarter.*rebounds'],
            'player_first_3_minutes_points': [r'player.*first.*3.*minutes.*points'],
            'player_first_3_minutes_assists': [r'player.*first.*3.*minutes.*assists'],
            'player_first_3_minutes_rebounds': [r'player.*first.*3.*minutes.*rebounds'],
        }

        # Player standard stats
        self.PLAYER_STAT_MARKETS = {
            # NBA
            'player_points': [r'player.*points', r'points$', r'^points\b', r'player.*pts'],
            'player_assists': [r'player.*assists', r'assists$', r'^assists\b', r'player.*ast'],
            'player_rebounds': [r'player.*rebounds', r'rebounds$', r'^rebounds\b', r'player.*reb'],
            'player_offensive_rebounds': [r'player.*offensive.*rebounds', r'offensive.*rebounds'],
            'player_defensive_rebounds': [r'player.*defensive.*rebounds', r'defensive.*rebounds'],
            'player_steals': [r'player.*steals', r'steals$', r'^steals\b', r'player.*stl'],
            'player_blocks': [r'player.*blocks', r'blocks$', r'^blocks\b', r'player.*blk'],
            'player_turnovers': [r'player.*turnovers', r'turnovers$', r'^turnovers\b', r'player.*to'],
            'player_threes': [r'player.*threes', r'player.*3.*point', r'threes$', r'3pm', r'player.*3pt'],
            'player_threes_attempted': [r'player.*threes.*attempted', r'3.*point.*attempted', r'3pa'],
            'player_twos': [r'player.*twos', r'twos$'],
            'player_twos_attempted': [r'player.*twos.*attempted'],
            'player_field_goals_made': [r'player.*field.*goals.*made', r'fgm'],
            'player_field_goals_attempted': [r'player.*field.*goals.*attempted', r'fga'],
            'player_free_throws_made': [r'player.*free.*throws.*made', r'ftm'],
            'player_free_throws_attempted': [r'player.*free.*throws.*attempted', r'fta'],
            'player_personal_fouls': [r'player.*personal.*fouls', r'fouls$', r'player.*pf'],
            'player_dunks': [r'player.*dunks', r'dunks$'],
            'player_fantasy_score': [r'player.*fantasy.*score', r'fantasy.*points'],

            # NFL
            'player_passing_yards': [r'player.*passing.*yards', r'passing.*yards', r'pass.*yds'],
            'player_passing_touchdowns': [r'player.*passing.*touchdowns', r'passing.*tds?'],
            'player_passing_interceptions': [r'player.*passing.*interceptions', r'interceptions', r'ints'],
            'player_passing_completions': [r'player.*passing.*completions', r'completions', r'comp'],
            'player_passing_attempts': [r'player.*passing.*attempts', r'attempts', r'att'],
            'player_rushing_yards': [r'player.*rushing.*yards', r'rushing.*yards', r'rush.*yds'],
            'player_rushing_touchdowns': [r'player.*rushing.*touchdowns', r'rushing.*tds?'],
            'player_rushing_attempts': [r'player.*rushing.*attempts', r'rushing.*att'],
            'player_receiving_yards': [r'player.*receiving.*yards', r'receiving.*yards', r'rec.*yds'],
            'player_receiving_touchdowns': [r'player.*receiving.*touchdowns', r'receiving.*tds?'],
            'player_receptions': [r'player.*receptions', r'receptions', r'rec$'],
            'player_targets': [r'player.*targets', r'targets$'],
            'player_touchdowns': [r'player.*touchdowns', r'touchdowns$', r'tds?$'],
            'player_yards': [r'player.*yards', r'yards$'],

            # NHL
            'player_goals': [r'player.*goals', r'goals$', r'^goals\b'],
            'player_assists': [r'player.*assists', r'assists$', r'^assists\b'],
            'player_points': [r'player.*points', r'points$', r'^points\b'],
            'player_shots_on_goal': [r'player.*shots.*on.*goal', r'shots.*on.*goal', r'sog', r'player.*shots'],
            'player_saves': [r'player.*saves', r'saves$', r'^saves\b'],
            'player_blocked_shots': [r'player.*blocked.*shots', r'blocked.*shots', r'blocks$'],
            'player_hits': [r'player.*hits', r'hits$'],
            'player_penalty_minutes': [r'player.*penalty.*minutes', r'pim', r'penalty.*mins'],
            'player_plus_minus': [r'player.*plus.*minus', r'plus/minus', r'\+/-'],
            'player_time_on_ice': [r'player.*time.*on.*ice', r'toi', r'ice.*time'],
            'player_faceoffs_won': [r'player.*faceoffs.*won', r'faceoffs', r'fow'],
            'player_goals_against': [r'player.*goals.*against', r'goals.*against', r'ga'],

            # MLB
            'player_hits': [r'player.*hits', r'hits$', r'^hits\b'],
            'player_runs': [r'player.*runs', r'runs$', r'^runs\b'],
            'player_rbi': [r'player.*rbi', r'rbi$', r'^rbi\b'],
            'player_home_runs': [r'player.*home.*runs', r'home.*runs', r'hr$', r'player.*hr'],
            'player_stolen_bases': [r'player.*stolen.*bases', r'stolen.*bases', r'sb$'],
            'player_walks': [r'player.*walks', r'walks$', r'bb$'],
            'player_strikeouts': [r'player.*strikeouts', r'strikeouts$', r'so$', r'k$'],
            'player_total_bases': [r'player.*total.*bases', r'total.*bases', r'tb$'],
            'player_extra_base_hits': [r'player.*extra.*base.*hits', r'extra.*base.*hits', r'xbh$'],
            'player_hit_by_pitch': [r'player.*hit.*by.*pitch', r'hbp$'],
            'player_sacrifice_flies': [r'player.*sacrifice.*flies', r'sac.*flies', r'sf$'],

            # WNBA (same as NBA but we'll keep separate entries for clarity)
            'player_points_wnba': [r'player.*points', r'points$'],
            'player_assists_wnba': [r'player.*assists', r'assists$'],
            'player_rebounds_wnba': [r'player.*rebounds', r'rebounds$'],
            'player_steals_wnba': [r'player.*steals', r'steals$'],
            'player_blocks_wnba': [r'player.*blocks', r'blocks$'],
            'player_threes_wnba': [r'player.*threes', r'3.*point', r'3pt'],

            # NCAAF (college football)
            'player_passing_yards_ncaaf': [r'player.*passing.*yards', r'passing.*yards'],
            'player_passing_touchdowns_ncaaf': [r'player.*passing.*touchdowns', r'passing.*tds'],
            'player_rushing_yards_ncaaf': [r'player.*rushing.*yards', r'rushing.*yards'],
            'player_rushing_touchdowns_ncaaf': [r'player.*rushing.*touchdowns', r'rushing.*tds'],
            'player_receiving_yards_ncaaf': [r'player.*receiving.*yards', r'receiving.*yards'],
            'player_receiving_touchdowns_ncaaf': [r'player.*receiving.*touchdowns', r'receiving.*tds'],
            'player_receptions_ncaaf': [r'player.*receptions', r'receptions$'],
            'player_longest_reception_ncaaf': [r'player.*longest.*reception', r'long.*rec'],
            'player_longest_rush_ncaaf': [r'player.*longest.*rush', r'long.*rush'],

            # NCAAB/NCAAW (college basketball - similar to NBA)
            'player_points_ncaab': [r'player.*points', r'points$'],
            'player_assists_ncaab': [r'player.*assists', r'assists$'],
            'player_rebounds_ncaab': [r'player.*rebounds', r'rebounds$'],
            'player_steals_ncaab': [r'player.*steals', r'steals$'],
            'player_blocks_ncaab': [r'player.*blocks', r'blocks$'],
            'player_threes_ncaab': [r'player.*threes', r'3.*point'],
            'player_turnovers_ncaab': [r'player.*turnovers', r'turnovers$'],
            'player_double_double_ncaab': [r'player.*double.*double', r'double.*double'],
            'player_triple_double_ncaab': [r'player.*triple.*double', r'triple.*double'],
        }

        # Player combo stats
        self.PLAYER_COMBO_MARKETS = {
            'player_points_assists': [r'player.*points.*\+.*assists', r'points.*assists', r'p\+a',
                                      r'points.*and.*assists'],
            'player_points_rebounds': [r'player.*points.*\+.*rebounds', r'points.*rebounds', r'p\+r',
                                       r'points.*and.*rebounds'],
            'player_points_rebounds_assists': [r'player.*points.*\+.*rebounds.*\+.*assists',
                                               r'points.*rebounds.*assists', r'pra', r'p\+r\+a'],
            'player_rebounds_assists': [r'player.*rebounds.*\+.*assists', r'rebounds.*assists', r'r\+a',
                                        r'rebounds.*and.*assists'],
            'player_blocks_steals': [r'player.*blocks.*\+.*steals', r'blocks.*steals', r'b\+s', r'blocks.*and.*steals'],
        }

        # Player special stats
        self.PLAYER_SPECIAL_MARKETS = {
            'player_double_double': [r'player.*double.*double', r'double.*double'],
            'player_triple_double': [r'player.*triple.*double', r'triple.*double'],
            'player_most_points': [r'player.*to.*have.*most.*points', r'most.*points'],
            'player_most_rebounds': [r'player.*to.*have.*most.*rebounds', r'most.*rebounds'],
            'player_most_assists': [r'player.*to.*have.*most.*assists', r'most.*assists'],
            'player_most_threes': [r'player.*to.*have.*most.*threes', r'most.*threes'],
            'player_most_points_rebounds_assists': [r'player.*to.*have.*most.*points.*rebounds.*assists', r'most.*pra'],
        }

        # First/Last scorer markets
        self.FIRST_LAST_SCORER_MARKETS = {
            'player_first_basket': [r'player.*first.*basket', r'first.*basket'],
            'player_first_field_goal': [r'player.*first.*field.*goal', r'first.*field.*goal'],
            'player_first_rebound': [r'player.*first.*rebound', r'first.*rebound'],
            'player_first_assist': [r'player.*first.*assist', r'first.*assist'],
            'player_first_three': [r'player.*first.*three', r'first.*three'],
            'player_first_dunk': [r'player.*first.*dunk', r'first.*dunk'],
            'player_first_touchdown': [r'player.*first.*touchdown', r'first.*touchdown.*scorer'],
            'player_last_touchdown': [r'player.*last.*touchdown', r'last.*touchdown.*scorer'],
            'player_first_goal': [r'player.*first.*goal', r'first.*goal'],
            'player_last_goal': [r'player.*last.*goal', r'last.*goal'],
            'team_player_first_basket': [r'team.*player.*first.*basket'],
            'team_player_first_field_goal': [r'team.*player.*first.*field.*goal'],
            'team_player_first_three': [r'team.*player.*first.*three'],
            'team_player_first_goal': [r'team.*player.*first.*goal'],
            'first_field_goal_type': [r'first.*field.*goal.*type'],
            'first_field_goal_type_4way': [r'first.*field.*goal.*type.*4.*way'],
            'player_first_field_goal_attempt_result_2way': [r'player.*first.*field.*goal.*attempt.*result.*2.*way'],
            'player_first_three_attempt_result_2way': [r'player.*first.*three.*attempt.*result.*2.*way'],
        }

        # Team markets
        self.TEAM_MARKETS = {
            'team_total_points': [r'team.*total.*points'],
            'team_total_points_odd_even': [r'team.*total.*points.*odd.*even'],
            'team_total_threes': [r'team.*total.*threes'],
            'team_1st_half_total': [r'team.*1st.*half.*total.*points'],
            'team_1st_half_total_odd_even': [r'team.*1st.*half.*total.*odd.*even'],
            'team_2nd_half_total': [r'team.*2nd.*half.*total.*points'],
            'team_2nd_half_total_odd_even': [r'team.*2nd.*half.*total.*odd.*even'],
            'team_1st_quarter_total': [r'team.*1st.*quarter.*total.*points'],
            'team_1st_quarter_total_odd_even': [r'team.*1st.*quarter.*total.*odd.*even'],
            'team_2nd_quarter_total': [r'team.*2nd.*quarter.*total.*points'],
            'team_2nd_quarter_total_odd_even': [r'team.*2nd.*quarter.*total.*odd.*even'],
            'team_3rd_quarter_total': [r'team.*3rd.*quarter.*total.*points'],
            'team_3rd_quarter_total_odd_even': [r'team.*3rd.*quarter.*total.*odd.*even'],
            'team_4th_quarter_excluding_ot_total': [r'team.*4th.*quarter.*excluding.*overtime.*total.*points'],
            'team_4th_quarter_including_ot_total': [r'team.*4th.*quarter.*including.*overtime.*total.*points'],
            'team_4th_quarter_including_ot_total_odd_even': [
                r'team.*4th.*quarter.*including.*overtime.*total.*odd.*even'],
            'team_1st_3_quarters_total': [r'team.*1st.*3.*quarters.*total.*points'],
            'team_highest_scoring_quarter_5way': [r'team.*highest.*scoring.*quarter.*5.*way'],
            'team_win_both_halves': [r'team.*win.*both.*halves'],
            'team_win_every_quarter': [r'team.*win.*every.*quarter'],
            'team_highest_three_point_percentage': [r'team.*to.*have.*highest.*three.*point.*field.*goal.*percentage'],
            'team_first_possession_result_5way': [r'team.*first.*possession.*result.*5.*way'],
            'team_first_possession_total_points': [r'team.*first.*possession.*total.*points'],
            'team_to_score_first_field_goal_2way': [r'team.*to.*score.*first.*field.*goal.*2.*way'],
            'team_to_score_last': [r'team.*to.*score.*last'],
            'team_to_win_opening_tip_off': [r'team.*to.*win.*opening.*tip.*off'],
            'team_first_3_minutes_total_threes': [r'team.*first.*3.*minutes.*total.*threes'],
        }

        # Game specials
        self.GAME_SPECIAL_MARKETS = {
            'first_minute_both_teams_score': [r'first.*minute.*both.*teams.*to.*score'],
            'highest_scoring_quarter_5way': [r'highest.*scoring.*quarter.*5.*way'],
            'lowest_scoring_quarter_5way': [r'lowest.*scoring.*quarter.*5.*way'],
            'highest_scoring_half_3way': [r'highest.*scoring.*half.*3.*way'],
            'largest_comeback': [r'largest.*comeback'],
            'will_there_be_overtime': [r'will.*there.*be.*overtime'],
            'first_3_minutes_total_threes': [r'first.*3.*minutes.*total.*threes'],
            'first_field_goal_type': [r'first.*field.*goal.*type'],
            'first_field_goal_type_4way': [r'first.*field.*goal.*type.*4.*way'],
            'total_threes': [r'total.*threes'],
            'team_to_score_last': [r'team.*to.*score.*last'],
        }

        # Game basic markets
        self.GAME_BASIC_MARKETS = {
            'moneyline': [r'^moneyline$', r'moneyline\b'],
            'point_spread': [r'point.*spread', r'spread$', r'^spread\b'],
            'total_points': [r'total.*points', r'over.*under', r'^total$'],
            'total_points_odd_even': [r'total.*points.*odd.*even', r'odd.*even'],
        }

        # Combine all market patterns
        self.ALL_MARKET_PATTERNS = {}
        self.ALL_MARKET_PATTERNS.update(self.QUARTER_MARKETS)
        self.ALL_MARKET_PATTERNS.update(self.HALF_MARKETS)
        self.ALL_MARKET_PATTERNS.update(self.FIRST_3_QUARTERS_MARKETS)
        self.ALL_MARKET_PATTERNS.update(self.FOURTH_QUARTER_MARKETS)
        self.ALL_MARKET_PATTERNS.update(self.REGULATION_MARKETS)
        self.ALL_MARKET_PATTERNS.update(self.PLAYER_PERIOD_STATS)
        self.ALL_MARKET_PATTERNS.update(self.PLAYER_STAT_MARKETS)
        self.ALL_MARKET_PATTERNS.update(self.PLAYER_COMBO_MARKETS)
        self.ALL_MARKET_PATTERNS.update(self.PLAYER_SPECIAL_MARKETS)
        self.ALL_MARKET_PATTERNS.update(self.FIRST_LAST_SCORER_MARKETS)
        self.ALL_MARKET_PATTERNS.update(self.TEAM_MARKETS)
        self.ALL_MARKET_PATTERNS.update(self.GAME_SPECIAL_MARKETS)
        self.ALL_MARKET_PATTERNS.update(self.GAME_BASIC_MARKETS)

        # Compile regex patterns for efficiency
        self.compiled_patterns = {}
        for market_type, patterns in self.ALL_MARKET_PATTERNS.items():
            self.compiled_patterns[market_type] = [re.compile(p, re.IGNORECASE) for p in patterns]

        # Period keywords for extraction
        self.PERIOD_KEYWORDS = {
            '1st quarter': ['1st quarter', 'first quarter', '1q', 'q1'],
            '2nd quarter': ['2nd quarter', 'second quarter', '2q', 'q2'],
            '3rd quarter': ['3rd quarter', 'third quarter', '3q', 'q3'],
            '4th quarter': ['4th quarter', 'fourth quarter', '4q', 'q4'],
            '1st half': ['1st half', 'first half', '1h', 'h1'],
            '2nd half': ['2nd half', 'second half', '2h', 'h2'],
            '1st period': ['1st period', 'first period'],
            '2nd period': ['2nd period', 'second period'],
            '3rd period': ['3rd period', 'third period'],
            '1st 3 quarters': ['1st 3 quarters', 'first 3 quarters'],
        }

        # Over/under markers
        self.OVER_UNDER_MARKERS = ['over', 'under', ' o ', ' u ', 'over ', 'under ']

        # Stat type mapping for subcategories
        self.STAT_TYPE_MAPPING = {
            # NBA
            'player_points': 'points',
            'player_assists': 'assists',
            'player_rebounds': 'rebounds',
            'player_offensive_rebounds': 'offensive_rebounds',
            'player_defensive_rebounds': 'defensive_rebounds',
            'player_steals': 'steals',
            'player_blocks': 'blocks',
            'player_turnovers': 'turnovers',
            'player_threes': 'three_pointers_made',
            'player_threes_attempted': 'three_pointers_attempted',
            'player_twos': 'two_pointers_made',
            'player_twos_attempted': 'two_pointers_attempted',
            'player_field_goals_made': 'field_goals_made',
            'player_field_goals_attempted': 'field_goals_attempted',
            'player_free_throws_made': 'free_throws_made',
            'player_free_throws_attempted': 'free_throws_attempted',
            'player_personal_fouls': 'personal_fouls',
            'player_dunks': 'dunks',
            'player_fantasy_score': 'fantasy_score',

            # NFL
            'player_passing_yards': 'passing_yards',
            'player_passing_touchdowns': 'passing_touchdowns',
            'player_passing_interceptions': 'interceptions',
            'player_passing_completions': 'completions',
            'player_passing_attempts': 'attempts',
            'player_rushing_yards': 'rushing_yards',
            'player_rushing_touchdowns': 'rushing_touchdowns',
            'player_rushing_attempts': 'rushing_attempts',
            'player_receiving_yards': 'receiving_yards',
            'player_receiving_touchdowns': 'receiving_touchdowns',
            'player_receptions': 'receptions',
            'player_targets': 'targets',
            'player_touchdowns': 'touchdowns',
            'player_yards': 'total_yards',

            # NHL
            'player_goals': 'goals',
            'player_assists': 'assists',
            'player_points': 'points',
            'player_shots_on_goal': 'shots_on_goal',
            'player_saves': 'saves',
            'player_blocked_shots': 'blocked_shots',
            'player_hits': 'hits',
            'player_penalty_minutes': 'penalty_minutes',
            'player_plus_minus': 'plus_minus',
            'player_time_on_ice': 'time_on_ice',
            'player_faceoffs_won': 'faceoffs_won',
            'player_goals_against': 'goals_against',

            # MLB
            'player_hits': 'hits',
            'player_runs': 'runs',
            'player_rbi': 'rbi',
            'player_home_runs': 'home_runs',
            'player_stolen_bases': 'stolen_bases',
            'player_walks': 'walks',
            'player_strikeouts': 'strikeouts',
            'player_total_bases': 'total_bases',
            'player_extra_base_hits': 'extra_base_hits',
            'player_hit_by_pitch': 'hit_by_pitch',
            'player_sacrifice_flies': 'sacrifice_flies',

            # WNBA (use same stat types as NBA)
            'player_points_wnba': 'points',
            'player_assists_wnba': 'assists',
            'player_rebounds_wnba': 'rebounds',
            'player_steals_wnba': 'steals',
            'player_blocks_wnba': 'blocks',
            'player_threes_wnba': 'three_pointers_made',

            # NCAAF
            'player_passing_yards_ncaaf': 'passing_yards',
            'player_passing_touchdowns_ncaaf': 'passing_touchdowns',
            'player_rushing_yards_ncaaf': 'rushing_yards',
            'player_rushing_touchdowns_ncaaf': 'rushing_touchdowns',
            'player_receiving_yards_ncaaf': 'receiving_yards',
            'player_receiving_touchdowns_ncaaf': 'receiving_touchdowns',
            'player_receptions_ncaaf': 'receptions',
            'player_longest_reception_ncaaf': 'longest_reception',
            'player_longest_rush_ncaaf': 'longest_rush',

            # NCAAB/NCAAW
            'player_points_ncaab': 'points',
            'player_assists_ncaab': 'assists',
            'player_rebounds_ncaab': 'rebounds',
            'player_steals_ncaab': 'steals',
            'player_blocks_ncaab': 'blocks',
            'player_threes_ncaab': 'three_pointers_made',
            'player_turnovers_ncaab': 'turnovers',
            'player_double_double_ncaab': 'double_double',
            'player_triple_double_ncaab': 'triple_double',

            # Combos
            'player_points_assists': 'points_assists',
            'player_points_rebounds': 'points_rebounds',
            'player_points_rebounds_assists': 'points_rebounds_assists',
            'player_rebounds_assists': 'rebounds_assists',
            'player_blocks_steals': 'blocks_steals',

            # Specials
            'player_double_double': 'double_double',
            'player_triple_double': 'triple_double',
            'player_most_points': 'most_points',
            'player_most_rebounds': 'most_rebounds',
            'player_most_assists': 'most_assists',
            'player_most_threes': 'most_threes',
            'player_most_points_rebounds_assists': 'most_pra',

            # Period stats
            'player_1st_quarter_points': 'first_quarter_points',
            'player_1st_quarter_assists': 'first_quarter_assists',
            'player_1st_quarter_rebounds': 'first_quarter_rebounds',
            'player_first_3_minutes_points': 'first_3_minutes_points',
            'player_first_3_minutes_assists': 'first_3_minutes_assists',
            'player_first_3_minutes_rebounds': 'first_3_minutes_rebounds',
        }

    def get_resolution_type(self, classification: MarketClassification) -> str:
        """
        Determine the type of resolution needed for this market
        Returns: resolution type string for the R script to fetch appropriate data
        """
        # Base category mapping
        category_map = {
            'player_stat_ou': 'player_stat',
            'player_stat': 'player_stat',
            'player_combo': 'player_combo',
            'player_special': 'player_special',
            'scorer': 'scorer',
            'first_last_scorer': 'first_last_scorer',
            'game': 'game_total',
            'game_special': 'game_special',
            'team_market': 'team_stat',
            'period_market': 'period_stat',
            'player_period_stat': 'player_period_stat',
        }

        # Get base resolution type
        resolution_type = category_map.get(classification.category, 'unknown')

        # ==============================================
        # FIRST/LAST SCORER MARKETS
        # ==============================================
        if classification.category == 'first_last_scorer':
            if 'touchdown' in classification.subcategory:
                if 'first' in classification.subcategory:
                    return 'first_touchdown_scorer'
                elif 'last' in classification.subcategory:
                    return 'last_touchdown_scorer'
            elif 'goal' in classification.subcategory:
                if 'first' in classification.subcategory:
                    return 'first_goal_scorer'
                elif 'last' in classification.subcategory:
                    return 'last_goal_scorer'
            elif 'basket' in classification.subcategory:
                if 'first' in classification.subcategory:
                    return 'first_basket_scorer'
            elif 'field_goal' in classification.subcategory:
                if 'first' in classification.subcategory:
                    return 'first_field_goal'
                elif 'type' in classification.subcategory:
                    return 'first_field_goal_type'
            elif 'assist' in classification.subcategory:
                if 'first' in classification.subcategory:
                    return 'first_assist'
            elif 'rebound' in classification.subcategory:
                if 'first' in classification.subcategory:
                    return 'first_rebound'
            elif 'three' in classification.subcategory:
                if 'first' in classification.subcategory:
                    return 'first_three'
            elif 'dunk' in classification.subcategory:
                if 'first' in classification.subcategory:
                    return 'first_dunk'

        # ==============================================
        # PLAYER SPECIAL MARKETS
        # ==============================================
        elif classification.category == 'player_special':
            if 'double_double' in classification.subcategory:
                return 'double_double'
            elif 'triple_double' in classification.subcategory:
                return 'triple_double'
            elif 'most_points' in classification.subcategory:
                return 'most_points'
            elif 'most_rebounds' in classification.subcategory:
                return 'most_rebounds'
            elif 'most_assists' in classification.subcategory:
                return 'most_assists'
            elif 'most_threes' in classification.subcategory:
                return 'most_threes'
            elif 'most_points_rebounds_assists' in classification.subcategory:
                return 'most_points_rebounds_assists'

        # ==============================================
        # PLAYER COMBO MARKETS
        # ==============================================
        elif classification.category == 'player_combo':
            if 'points_assists' in classification.subcategory:
                return 'player_points_assists'
            elif 'points_rebounds' in classification.subcategory:
                return 'player_points_rebounds'
            elif 'points_rebounds_assists' in classification.subcategory:
                return 'player_points_rebounds_assists'
            elif 'rebounds_assists' in classification.subcategory:
                return 'player_rebounds_assists'
            elif 'blocks_steals' in classification.subcategory:
                return 'player_blocks_steals'

        # ==============================================
        # PLAYER STAT MARKETS
        # ==============================================
        elif classification.category in ['player_stat', 'player_stat_ou']:
            stat_mapping = {
                # NBA
                'player_points': 'points',
                'player_assists': 'assists',
                'player_rebounds': 'rebounds',
                'player_offensive_rebounds': 'offensive_rebounds',
                'player_defensive_rebounds': 'defensive_rebounds',
                'player_steals': 'steals',
                'player_blocks': 'blocks',
                'player_turnovers': 'turnovers',
                'player_threes': 'three_pointers',
                'player_threes_attempted': 'three_pointers_attempted',
                'player_twos': 'two_pointers',
                'player_twos_attempted': 'two_pointers_attempted',
                'player_field_goals_made': 'field_goals',
                'player_field_goals_attempted': 'field_goals_attempted',
                'player_free_throws_made': 'free_throws',
                'player_free_throws_attempted': 'free_throws_attempted',
                'player_personal_fouls': 'fouls',
                'player_dunks': 'dunks',
                'player_fantasy_score': 'fantasy_score',

                # NFL
                'player_passing_yards': 'passing_yards',
                'player_passing_touchdowns': 'passing_touchdowns',
                'player_passing_interceptions': 'interceptions',
                'player_passing_completions': 'completions',
                'player_passing_attempts': 'passing_attempts',
                'player_rushing_yards': 'rushing_yards',
                'player_rushing_touchdowns': 'rushing_touchdowns',
                'player_rushing_attempts': 'rushing_attempts',
                'player_receiving_yards': 'receiving_yards',
                'player_receiving_touchdowns': 'receiving_touchdowns',
                'player_receptions': 'receptions',
                'player_targets': 'targets',
                'player_touchdowns': 'touchdowns',
                'player_yards': 'total_yards',

                # NHL
                'player_goals': 'goals',
                'player_assists': 'assists',
                'player_points': 'points',
                'player_shots_on_goal': 'shots_on_goal',
                'player_saves': 'saves',
                'player_blocked_shots': 'blocked_shots',
                'player_hits': 'hits',
                'player_penalty_minutes': 'penalty_minutes',
                'player_plus_minus': 'plus_minus',
                'player_time_on_ice': 'time_on_ice',
                'player_faceoffs_won': 'faceoffs_won',
                'player_goals_against': 'goals_against',

                # MLB
                'player_hits': 'hits',
                'player_runs': 'runs',
                'player_rbi': 'rbi',
                'player_home_runs': 'home_runs',
                'player_stolen_bases': 'stolen_bases',
                'player_walks': 'walks',
                'player_strikeouts': 'strikeouts',
                'player_total_bases': 'total_bases',
                'player_extra_base_hits': 'extra_base_hits',
                'player_hit_by_pitch': 'hit_by_pitch',
                'player_sacrifice_flies': 'sacrifice_flies',

                # WNBA
                'player_points_wnba': 'points',
                'player_assists_wnba': 'assists',
                'player_rebounds_wnba': 'rebounds',
                'player_steals_wnba': 'steals',
                'player_blocks_wnba': 'blocks',
                'player_threes_wnba': 'three_pointers',

                # NCAAF
                'player_passing_yards_ncaaf': 'passing_yards',
                'player_passing_touchdowns_ncaaf': 'passing_touchdowns',
                'player_rushing_yards_ncaaf': 'rushing_yards',
                'player_rushing_touchdowns_ncaaf': 'rushing_touchdowns',
                'player_receiving_yards_ncaaf': 'receiving_yards',
                'player_receiving_touchdowns_ncaaf': 'receiving_touchdowns',
                'player_receptions_ncaaf': 'receptions',
                'player_longest_reception_ncaaf': 'longest_reception',
                'player_longest_rush_ncaaf': 'longest_rush',

                # NCAAB/NCAAW
                'player_points_ncaab': 'points',
                'player_assists_ncaab': 'assists',
                'player_rebounds_ncaab': 'rebounds',
                'player_steals_ncaab': 'steals',
                'player_blocks_ncaab': 'blocks',
                'player_threes_ncaab': 'three_pointers',
                'player_turnovers_ncaab': 'turnovers',
                'player_double_double_ncaab': 'double_double',
                'player_triple_double_ncaab': 'triple_double',
            }

            stat_type = stat_mapping.get(classification.subcategory, 'player_stat')
            return f'player_{stat_type}'

        # ==============================================
        # TEAM MARKETS
        # ==============================================
        elif classification.category == 'team_market':
            if 'total_points' in classification.subcategory:
                if '1st_quarter' in classification.subcategory:
                    return 'team_1st_quarter_points'
                elif '2nd_quarter' in classification.subcategory:
                    return 'team_2nd_quarter_points'
                elif '3rd_quarter' in classification.subcategory:
                    return 'team_3rd_quarter_points'
                elif '4th_quarter' in classification.subcategory:
                    if 'excluding_ot' in classification.subcategory:
                        return 'team_4th_quarter_excluding_ot_points'
                    elif 'including_ot' in classification.subcategory:
                        return 'team_4th_quarter_including_ot_points'
                    else:
                        return 'team_4th_quarter_points'
                elif '1st_half' in classification.subcategory:
                    return 'team_1st_half_points'
                elif '2nd_half' in classification.subcategory:
                    return 'team_2nd_half_points'
                elif '1st_3_quarters' in classification.subcategory:
                    return 'team_1st_3_quarters_points'
                else:
                    return 'team_total_points'
            elif 'threes' in classification.subcategory:
                if 'first_3_minutes' in classification.subcategory:
                    return 'team_first_3_minutes_threes'
                else:
                    return 'team_total_threes'
            elif 'first_possession' in classification.subcategory:
                if 'result' in classification.subcategory:
                    return 'team_first_possession_result'
                else:
                    return 'team_first_possession_points'
            elif 'win_both_halves' in classification.subcategory:
                return 'team_win_both_halves'
            elif 'win_every_quarter' in classification.subcategory:
                return 'team_win_every_quarter'
            elif 'highest_scoring_quarter' in classification.subcategory:
                return 'team_highest_scoring_quarter'
            elif 'opening_tip' in classification.subcategory:
                return 'team_opening_tip'
            elif 'score_last' in classification.subcategory:
                return 'team_score_last'
            elif 'highest_three_point_percentage' in classification.subcategory:
                return 'team_highest_three_point_percentage'
            elif 'score_first_field_goal' in classification.subcategory:
                return 'team_score_first_field_goal'

        # ==============================================
        # PERIOD MARKETS
        # ==============================================
        elif classification.category == 'period_market':
            if 'moneyline' in classification.subcategory:
                if '1st_quarter' in classification.subcategory:
                    return '1st_quarter_moneyline'
                elif '2nd_quarter' in classification.subcategory:
                    return '2nd_quarter_moneyline'
                elif '3rd_quarter' in classification.subcategory:
                    return '3rd_quarter_moneyline'
                elif '4th_quarter' in classification.subcategory:
                    if 'excluding_ot' in classification.subcategory:
                        return '4th_quarter_excluding_ot_moneyline'
                    elif 'including_ot' in classification.subcategory:
                        return '4th_quarter_including_ot_moneyline'
                    else:
                        return '4th_quarter_moneyline'
                elif '1st_half' in classification.subcategory:
                    return '1st_half_moneyline'
                elif '2nd_half' in classification.subcategory:
                    if 'excluding_ot' in classification.subcategory:
                        return '2nd_half_excluding_ot_moneyline'
                    else:
                        return '2nd_half_moneyline'
                elif '1st_3_quarters' in classification.subcategory:
                    return '1st_3_quarters_moneyline'

            elif 'spread' in classification.subcategory:
                if '1st_quarter' in classification.subcategory:
                    return '1st_quarter_spread'
                elif '2nd_quarter' in classification.subcategory:
                    return '2nd_quarter_spread'
                elif '3rd_quarter' in classification.subcategory:
                    return '3rd_quarter_spread'
                elif '4th_quarter' in classification.subcategory:
                    if 'excluding_ot' in classification.subcategory:
                        return '4th_quarter_excluding_ot_spread'
                    elif 'including_ot' in classification.subcategory:
                        return '4th_quarter_including_ot_spread'
                    else:
                        return '4th_quarter_spread'
                elif '1st_half' in classification.subcategory:
                    return '1st_half_spread'
                elif '2nd_half' in classification.subcategory:
                    return '2nd_half_spread'
                elif '1st_3_quarters' in classification.subcategory:
                    return '1st_3_quarters_spread'

            elif 'total' in classification.subcategory:
                if '1st_quarter' in classification.subcategory:
                    return '1st_quarter_total'
                elif '2nd_quarter' in classification.subcategory:
                    return '2nd_quarter_total'
                elif '3rd_quarter' in classification.subcategory:
                    return '3rd_quarter_total'
                elif '4th_quarter' in classification.subcategory:
                    if 'excluding_ot' in classification.subcategory:
                        return '4th_quarter_excluding_ot_total'
                    elif 'including_ot' in classification.subcategory:
                        return '4th_quarter_including_ot_total'
                    else:
                        return '4th_quarter_total'
                elif '1st_half' in classification.subcategory:
                    return '1st_half_total'
                elif '2nd_half' in classification.subcategory:
                    return '2nd_half_total'
                elif '1st_3_quarters' in classification.subcategory:
                    return '1st_3_quarters_total'

        # ==============================================
        # GAME MARKETS
        # ==============================================
        elif classification.category == 'game':
            if classification.subcategory == 'moneyline':
                return 'moneyline'
            elif classification.subcategory == 'point_spread':
                return 'point_spread'
            elif classification.subcategory == 'total_points':
                return 'total_points'
            elif classification.subcategory == 'total_points_odd_even':
                return 'total_points_odd_even'
            elif classification.subcategory == 'regulation_moneyline_3way':
                return 'regulation_moneyline_3way'

        # ==============================================
        # GAME SPECIAL MARKETS
        # ==============================================
        elif classification.category == 'game_special':
            special_mapping = {
                'first_minute_both_teams_score': 'first_minute_both_teams_score',
                'highest_scoring_quarter_5way': 'highest_scoring_quarter',
                'lowest_scoring_quarter_5way': 'lowest_scoring_quarter',
                'highest_scoring_half_3way': 'highest_scoring_half',
                'largest_comeback': 'largest_comeback',
                'will_there_be_overtime': 'overtime',
                'first_3_minutes_total_threes': 'first_3_minutes_threes',
                'first_field_goal_type': 'first_field_goal_type',
                'first_field_goal_type_4way': 'first_field_goal_type',
                'total_threes': 'total_threes',
                'team_to_score_last': 'team_score_last',
            }
            return special_mapping.get(classification.subcategory, 'game_special')

        # ==============================================
        # PLAYER PERIOD STATS
        # ==============================================
        elif classification.category == 'player_period_stat':
            if '1st_quarter_points' in classification.subcategory:
                return 'player_1st_quarter_points'
            elif '1st_quarter_assists' in classification.subcategory:
                return 'player_1st_quarter_assists'
            elif '1st_quarter_rebounds' in classification.subcategory:
                return 'player_1st_quarter_rebounds'
            elif 'first_3_minutes_points' in classification.subcategory:
                return 'player_first_3_minutes_points'
            elif 'first_3_minutes_assists' in classification.subcategory:
                return 'player_first_3_minutes_assists'
            elif 'first_3_minutes_rebounds' in classification.subcategory:
                return 'player_first_3_minutes_rebounds'

        return resolution_type

    def classify(self, market: str, player_or_team: str) -> MarketClassification:
        """
        Main classification method for ANY market from your list
        Returns: MarketClassification object with all extracted info
        """
        market_lower = market.lower()
        player_lower = (player_or_team or "").lower()

        logger.debug(f"Classifying market: '{market}' | player/team: '{player_or_team}'")

        # Step 1: Extract period information
        period = self._extract_period(market_lower)

        # Step 2: Extract direction and line value
        direction, line_value = self._extract_direction_and_line(player_or_team, market_lower)

        # Step 3: Extract clean player/team names
        clean_player, clean_team = self._extract_clean_names(player_or_team, direction, line_value)

        # Step 4: Determine category and subcategory
        category, subcategory = self._determine_category(market_lower, player_lower)

        # Step 5: Extract stat type
        stat_type = self.STAT_TYPE_MAPPING.get(subcategory)

        # Step 6: Get confidence score
        confidence = self.CONFIDENCE_SCORES.get(category, 0.5)

        logger.info(f"Market classified: {category}.{subcategory}")
        logger.info(f"  Player: '{clean_player}', Team: '{clean_team}'")
        logger.info(f"  Direction: {direction}, Line: {line_value}")
        logger.info(f"  Period: {period}, Stat: {stat_type}")
        logger.info(f"  Confidence: {confidence:.2f}")

        return MarketClassification(
            category=category,
            subcategory=subcategory,
            clean_player=clean_player,
            clean_team=clean_team,
            direction=direction,
            line_value=line_value,
            period=period,
            stat_type=stat_type,
            original_player=player_or_team,
            original_market=market,
            confidence=confidence
        )

    def _extract_period(self, market: str) -> Optional[str]:
        """Extract period/quarter/half information from market"""
        for period_name, keywords in self.PERIOD_KEYWORDS.items():
            for keyword in keywords:
                if keyword in market:
                    return period_name
        return None

    def _extract_direction_and_line(self, player_string: str, market: str) -> Tuple[Optional[str], Optional[float]]:
        """Extract over/under direction and line value"""
        combined_string = f"{player_string} {market}"

        # Patterns for extraction
        patterns = [
            (r'([Oo]ver|[Uu]nder)\s+(\d+\.?\d*)', 'full'),
            (r'\b([Oo]|[Uu])\s+(\d+\.?\d*)', 'short'),
            (r'([+-]\d+\.?\d*)', 'spread'),
            (r'(\d+\.?\d*)$', 'number_only'),
        ]

        direction = None
        line_value = None

        for pattern, pattern_type in patterns:
            match = re.search(pattern, combined_string)
            if match:
                if pattern_type == 'full':
                    direction = 'over' if match.group(1).lower() == 'over' else 'under'
                    line_value = float(match.group(2))
                elif pattern_type == 'short':
                    direction = 'over' if match.group(1).lower() == 'o' else 'under'
                    line_value = float(match.group(2))
                elif pattern_type == 'spread':
                    line_value = float(match.group(1))
                    direction = 'spread'
                elif pattern_type == 'number_only':
                    line_value = float(match.group(1))

                if direction or line_value:
                    break

        # If no direction found but we have a line, check market for direction
        if line_value and not direction:
            if 'over' in market.lower():
                direction = 'over'
            elif 'under' in market.lower():
                direction = 'under'

        return direction, line_value

    def _extract_clean_names(self, player_string: str, direction: Optional[str],
                             line_value: Optional[float]) -> Tuple[str, str]:
        """Extract clean player and team names"""
        if not player_string:
            return "", ""

        clean = player_string

        # Remove patterns
        patterns_to_remove = [
            r'\s+[Oo]ver\s+\d+\.?\d*\s*',
            r'\s+[Uu]nder\s+\d+\.?\d*\s*',
            r'\s+[Oo]\s+\d+\.?\d*\s*',
            r'\s+[Uu]\s+\d+\.?\d*\s*',
            r'\s+\+\d+\.?\d*\s*',
            r'\s+-\d+\.?\d*\s*',
            r'\s+\d+\.?\d*\s*$',
            r'\s*\(.*?\)\s*',
            r'\s+Yes\s*$',
            r'\s+No\s*$',
        ]

        for pattern in patterns_to_remove:
            clean = re.sub(pattern, ' ', clean)

        clean = re.sub(r'\s+', ' ', clean).strip()

        # Check for "Team Player Name" pattern
        team_names = self._get_team_names()
        clean_lower = clean.lower()

        # First check if the entire string is a known team
        for team_name in team_names:
            if team_name == clean_lower:
                return "", clean

        # Try to find a team name at the beginning
        best_match = ("", "")
        best_match_length = 0
        MIN_TEAM_NAME_LENGTH = 4

        for team_name in team_names:
            if len(team_name) < MIN_TEAM_NAME_LENGTH:
                continue

            if clean_lower.startswith(team_name):
                if len(team_name) > best_match_length:
                    if len(clean_lower) == len(team_name) or clean_lower[len(team_name)] == ' ':
                        team_part = clean[:len(team_name)]
                        player_part = clean[len(team_name):].strip()
                        player_part = re.sub(r'^\s*[-\s]+\s*|\s*[-\s]+\s*$', '', player_part).strip()

                        if player_part:
                            best_match = (player_part, team_part)
                            best_match_length = len(team_name)

        if best_match[0] or best_match[1]:
            return best_match

        # Check for team name in the middle
        for team_name in team_names:
            if len(team_name) < MIN_TEAM_NAME_LENGTH:
                continue

            if team_name in clean_lower:
                parts = re.split(team_name, clean_lower, flags=re.IGNORECASE)
                if len(parts) == 2:
                    player_part = parts[0].strip()
                    team_part = clean[clean_lower.index(team_name):clean_lower.index(team_name) + len(team_name)]
                    player_part = re.sub(r'[-\s]+$', '', player_part).strip()

                    if player_part:
                        return player_part, team_part

        return clean, ""

    def _determine_category(self, market: str, player: str) -> Tuple[str, str]:
        """
        Determine the main category and subcategory by checking all market patterns
        """
        market_lower = market.lower()

        # Check each pattern group in priority order using COMPILED patterns
        for subcategory, patterns in self.FIRST_LAST_SCORER_MARKETS.items():
            for pattern in patterns:
                # FIX: Use the compiled pattern from self.compiled_patterns
                compiled = self.compiled_patterns.get(subcategory, [])
                for cp in compiled:
                    if cp.search(market_lower):
                        return 'first_last_scorer', subcategory

        # Player combo markets
        for subcategory, patterns in self.PLAYER_COMBO_MARKETS.items():
            for pattern in patterns:
                compiled = self.compiled_patterns.get(subcategory, [])
                for cp in compiled:
                    if cp.search(market_lower):
                        return 'player_combo', subcategory

        # Player special markets
        for subcategory, patterns in self.PLAYER_SPECIAL_MARKETS.items():
            for pattern in patterns:
                compiled = self.compiled_patterns.get(subcategory, [])
                for cp in compiled:
                    if cp.search(market_lower):
                        return 'player_special', subcategory

        # Player period stats
        for subcategory, patterns in self.PLAYER_PERIOD_STATS.items():
            for pattern in patterns:
                compiled = self.compiled_patterns.get(subcategory, [])
                for cp in compiled:
                    if cp.search(market_lower):
                        return 'player_period_stat', subcategory

        # Quarter markets
        for subcategory, patterns in self.QUARTER_MARKETS.items():
            for pattern in patterns:
                compiled = self.compiled_patterns.get(subcategory, [])
                for cp in compiled:
                    if cp.search(market_lower):
                        return 'period_market', subcategory

        # Half markets
        for subcategory, patterns in self.HALF_MARKETS.items():
            for pattern in patterns:
                compiled = self.compiled_patterns.get(subcategory, [])
                for cp in compiled:
                    if cp.search(market_lower):
                        return 'period_market', subcategory

        # First 3 Quarters markets
        for subcategory, patterns in self.FIRST_3_QUARTERS_MARKETS.items():
            for pattern in patterns:
                compiled = self.compiled_patterns.get(subcategory, [])
                for cp in compiled:
                    if cp.search(market_lower):
                        return 'period_market', subcategory

        # 4th Quarter markets
        for subcategory, patterns in self.FOURTH_QUARTER_MARKETS.items():
            for pattern in patterns:
                compiled = self.compiled_patterns.get(subcategory, [])
                for cp in compiled:
                    if cp.search(market_lower):
                        return 'period_market', subcategory

        # Regulation markets
        for subcategory, patterns in self.REGULATION_MARKETS.items():
            for pattern in patterns:
                compiled = self.compiled_patterns.get(subcategory, [])
                for cp in compiled:
                    if cp.search(market_lower):
                        return 'game', subcategory

        # Team markets
        for subcategory, patterns in self.TEAM_MARKETS.items():
            for pattern in patterns:
                compiled = self.compiled_patterns.get(subcategory, [])
                for cp in compiled:
                    if cp.search(market_lower):
                        return 'team_market', subcategory

        # Game special markets
        for subcategory, patterns in self.GAME_SPECIAL_MARKETS.items():
            for pattern in patterns:
                compiled = self.compiled_patterns.get(subcategory, [])
                for cp in compiled:
                    if cp.search(market_lower):
                        return 'game_special', subcategory

        # Game basic markets
        for subcategory, patterns in self.GAME_BASIC_MARKETS.items():
            for pattern in patterns:
                compiled = self.compiled_patterns.get(subcategory, [])
                for cp in compiled:
                    if cp.search(market_lower):
                        return 'game', subcategory

        # Player stat markets (check for over/under)
        for subcategory, patterns in self.PLAYER_STAT_MARKETS.items():
            for pattern in patterns:
                compiled = self.compiled_patterns.get(subcategory, [])
                for cp in compiled:
                    if cp.search(market_lower):
                        # Check if it's over/under
                        if any(ou in market_lower or ou in player for ou in self.OVER_UNDER_MARKERS):
                            return 'player_stat_ou', subcategory
                        return 'player_stat', subcategory

        # Default
        return 'player_prop', 'generic'

    def get_market_for_r_script(self, classification: MarketClassification) -> str:
        """
        Get the market string to send to R script
        Returns the ORIGINAL market name for R to parse
        """
        # For first/last scorer markets, use specific names for R
        if classification.category == 'first_last_scorer':
            if 'first' in classification.subcategory:
                if 'goal' in classification.subcategory:
                    return 'Player First Goal'
                elif 'basket' in classification.subcategory:
                    return 'Player First Basket'
                elif 'touchdown' in classification.subcategory:
                    return 'Player First Touchdown Scorer'
                elif 'field_goal' in classification.subcategory:
                    return 'Player First Field Goal'
                elif 'assist' in classification.subcategory:
                    return 'Player First Assist'
                elif 'rebound' in classification.subcategory:
                    return 'Player First Rebound'
                elif 'three' in classification.subcategory:
                    return 'Player First Three'
                elif 'dunk' in classification.subcategory:
                    return 'Player First Dunk'
                else:
                    return 'Player First Goal'
            elif 'last' in classification.subcategory:
                if 'goal' in classification.subcategory:
                    return 'Player Last Goal'
                elif 'touchdown' in classification.subcategory:
                    return 'Player Last Touchdown Scorer'
                else:
                    return 'Player Last Goal'

        # For combo markets, use full names
        if classification.category == 'player_combo':
            combo_mapping = {
                'player_points_assists': 'Player Points + Assists',
                'player_points_rebounds': 'Player Points + Rebounds',
                'player_points_rebounds_assists': 'Player Points + Rebounds + Assists',
                'player_rebounds_assists': 'Player Rebounds + Assists',
                'player_blocks_steals': 'Player Blocks + Steals',
            }
            return combo_mapping.get(classification.subcategory, classification.original_market)

        # For period markets, preserve the period info
        if classification.category == 'period_market':
            # Map to appropriate market for R
            period_mapping = {
                '1st_quarter_moneyline': '1st Quarter Moneyline',
                '1st_quarter_spread': '1st Quarter Point Spread',
                '1st_quarter_total': '1st Quarter Total Points',
                '1st_half_moneyline': '1st Half Moneyline',
                '1st_half_spread': '1st Half Point Spread',
                '1st_half_total': '1st Half Total Points',
                '2nd_quarter_moneyline': '2nd Quarter Moneyline',
                '2nd_quarter_spread': '2nd Quarter Point Spread',
                '2nd_quarter_total': '2nd Quarter Total Points',
                '3rd_quarter_moneyline': '3rd Quarter Moneyline',
                '3rd_quarter_spread': '3rd Quarter Point Spread',
                '3rd_quarter_total': '3rd Quarter Total Points',
                '4th_quarter_moneyline': '4th Quarter Moneyline',
                '4th_quarter_spread': '4th Quarter Point Spread',
                '4th_quarter_total': '4th Quarter Total Points',
            }
            return period_mapping.get(classification.subcategory, classification.original_market)

        # For all others, return the original market name
        return classification.original_market

    def _get_team_names(self) -> List[str]:
        """Get list of common team names for ALL sports"""
        return [
            # NBA Teams
            'los angeles lakers', 'golden state warriors', 'boston celtics', 'miami heat',
            'chicago bulls', 'new york knicks', 'dallas mavericks', 'denver nuggets',
            'phoenix suns', 'milwaukee bucks', 'philadelphia 76ers', 'los angeles clippers',
            'brooklyn nets', 'toronto raptors', 'atlanta hawks', 'washington wizards',
            'charlotte hornets', 'detroit pistons', 'indiana pacers', 'cleveland cavaliers',
            'orlando magic', 'minnesota timberwolves', 'oklahoma city thunder',
            'portland trail blazers', 'sacramento kings', 'san antonio spurs',
            'houston rockets', 'utah jazz', 'new orleans pelicans', 'memphis grizzlies',
            'lakers', 'warriors', 'celtics', 'heat', 'bulls', 'knicks',
            'mavericks', 'nuggets', 'suns', 'bucks', '76ers', 'clippers',
            'nets', 'raptors', 'hawks', 'wizards', 'hornets', 'pistons',
            'pacers', 'cavaliers', 'magic', 'timberwolves', 'thunder',
            'trail blazers', 'kings', 'spurs', 'rockets', 'jazz', 'pelicans', 'grizzlies',

            # NFL Teams
            'kansas city chiefs', 'san francisco 49ers', 'philadelphia eagles',
            'dallas cowboys', 'baltimore ravens', 'green bay packers', 'buffalo bills',
            'miami dolphins', 'new york jets', 'new england patriots', 'pittsburgh steelers',
            'seattle seahawks', 'los angeles rams', 'cincinnati bengals', 'minnesota vikings',
            'new orleans saints', 'new york giants', 'washington commanders',
            'denver broncos', 'las vegas raiders', 'los angeles chargers', 'cleveland browns',
            'tennessee titans', 'jacksonville jaguars', 'atlanta falcons', 'carolina panthers',
            'chicago bears', 'arizona cardinals', 'indianapolis colts', 'tampa bay buccaneers',
            'chiefs', '49ers', 'eagles', 'cowboys', 'ravens', 'packers',
            'bills', 'dolphins', 'jets', 'patriots', 'steelers', 'seahawks',
            'rams', 'bengals', 'vikings', 'saints', 'giants', 'commanders',
            'broncos', 'raiders', 'chargers', 'browns', 'titans', 'jaguars',
            'falcons', 'panthers', 'bears', 'cardinals', 'colts', 'buccaneers',

            # NHL Teams
            'chicago blackhawks', 'boston bruins', 'buffalo sabres', 'calgary flames',
            'carolina hurricanes', 'colorado avalanche', 'columbus blue jackets',
            'dallas stars', 'detroit red wings', 'edmonton oilers', 'florida panthers',
            'los angeles kings', 'minnesota wild', 'montreal canadiens', 'nashville predators',
            'new jersey devils', 'new york islanders', 'new york rangers', 'ottawa senators',
            'philadelphia flyers', 'arizona coyotes', 'pittsburgh penguins', 'san jose sharks',
            'st. louis blues', 'st louis blues', 'tampa bay lightning', 'toronto maple leafs',
            'vancouver canucks', 'vegas golden knights', 'washington capitals', 'winnipeg jets',
            'blackhawks', 'bruins', 'sabres', 'flames', 'hurricanes', 'avalanche',
            'blue jackets', 'stars', 'red wings', 'oilers', 'panthers', 'kings',
            'wild', 'canadiens', 'predators', 'devils', 'islanders', 'rangers',
            'senators', 'flyers', 'coyotes', 'penguins', 'sharks', 'blues',
            'lightning', 'maple leafs', 'canucks', 'golden knights', 'capitals', 'jets',

            # MLB Teams
            'new york yankees', 'boston red sox', 'los angeles dodgers', 'chicago cubs',
            'san francisco giants', 'houston astros', 'atlanta braves', 'st louis cardinals',
            'new york mets', 'philadelphia phillies', 'toronto blue jays', 'chicago white sox',
            'cleveland guardians', 'detroit tigers', 'kansas city royals', 'minnesota twins',
            'tampa bay rays', 'baltimore orioles', 'oakland athletics', 'seattle mariners',
            'texas rangers', 'arizona diamondbacks', 'colorado rockies', 'miami marlins',
            'milwaukee brewers', 'cincinnati reds', 'pittsburgh pirates', 'san diego padres',
            'washington nationals', 'los angeles angels', 'yankees', 'red sox', 'dodgers',
            'cubs', 'giants', 'astros', 'braves', 'cardinals', 'mets', 'phillies', 'blue jays',

            # WNBA Teams
            'las vegas aces', 'new york liberty', 'chicago sky', 'seattle storm',
            'connecticut sun', 'phoenix mercury', 'dallas wings', 'atlanta dream',
            'minnesota lynx', 'los angeles sparks', 'washington mystics', 'indiana fever',
            'aces', 'liberty', 'sky', 'storm', 'sun', 'mercury', 'wings', 'dream',

            # NCAAF Teams (college football)
            'alabama', 'clemson', 'ohio state', 'georgia', 'lsu', 'oklahoma', 'notre dame',
            'texas', 'texas a&m', 'florida', 'florida state', 'miami', 'michigan', 'usc',
            'oregon', 'penn state', 'wisconsin', 'iowa', 'auburn', 'tennessee', 'utah',
            'baylor', 'oklahoma state', 'cincinnati', 'houston', 'byu', 'ucf',
            'alabama crimson tide', 'clemson tigers', 'ohio state buckeyes', 'georgia bulldogs',
            'lsu tigers', 'oklahoma sooners', 'notre dame fighting irish', 'texas longhorns',
            'texas a&m aggies', 'florida gators', 'florida state seminoles', 'miami hurricanes',
            'michigan wolverines', 'usc trojans', 'oregon ducks', 'penn state nittany lions',
            'wisconsin badgers', 'iowa hawkeyes', 'auburn tigers', 'tennessee volunteers',
            'utah utes', 'baylor bears', 'oklahoma state cowboys', 'cincinnati bearcats',

            # NCAAB Teams (college basketball - men's)
            'duke', 'north carolina', 'kansas', 'kentucky', 'ucla', 'gonzaga', 'arizona',
            'indiana', 'michigan state', 'illinois', 'purdue', 'villanova', 'connecticut',
            'syracuse', 'louisville', 'arkansas', 'texas tech', 'west virginia',
            'duke blue devils', 'north carolina tar heels', 'kansas jayhawks',
            'kentucky wildcats', 'ucla bruins', 'gonzaga bulldogs', 'arizona wildcats',
            'indiana hoosiers', 'michigan state spartans', 'illinois fighting illini',
            'purdue boilermakers', 'villanova wildcats', 'uconn huskies',

            # NCAAW Teams (college basketball - women's)
            'south carolina', 'uconn', 'stanford', 'louisville', 'north carolina state',
            'tennessee', 'baylor', 'notre dame', 'maryland', 'texas', 'oregon state',
            'south carolina gamecocks', 'uconn huskies', 'stanford cardinal',
            'louisville cardinals', 'nc state wolfpack', 'tennessee volunteers',
            'baylor bears', 'notre dame fighting irish', 'maryland terrapins',
            'texas longhorns', 'oregon state beavers',
        ]

    def to_dict(self, classification: MarketClassification) -> Dict:
        """Convert MarketClassification to dictionary for compatibility"""
        return {
            'category': classification.category,
            'subcategory': classification.subcategory,
            'clean_player': classification.clean_player,
            'clean_team': classification.clean_team,
            'direction': classification.direction,
            'line_value': classification.line_value,
            'period': classification.period,
            'stat_type': classification.stat_type,
            'original_player': classification.original_player,
            'original_market': classification.original_market,
            'confidence': classification.confidence
        }


# Test function
def test_market_classifier():
    """Test the market classifier with various markets from your list"""
    classifier = MarketClassifier()

    test_cases = [
        # NHL SCORER MARKETS
        ("Team Player First Goal Scorer", "Chicago Blackhawks Colton Dach"),
        ("Player First Goal", "Colton Dach"),
        ("First Goal Scorer", "Mark Scheifele"),
        ("Player Last Goal", "Connor McDavid"),

        # NHL Player Stats
        ("Player Goals Over 0.5", "Alex Ovechkin"),
        ("Player Assists Under 1.5", "Connor McDavid"),
        ("Player Shots On Goal Over 4.5", "Auston Matthews"),
        ("Player Saves Over 28.5", "Andrei Vasilevskiy"),

        # COMBO MARKETS
        ("Player Points + Rebounds + Assists Over 40.5", "Joel Embiid"),
        ("Player Points + Rebounds Under 15.5", "Stephen Curry"),
        ("Player Points + Assists Over 25.5", "Luka Doncic"),
        ("Player Rebounds + Assists Under 20.5", "Nikola Jokic"),
        ("Player Blocks + Steals Over 2.5", "Anthony Davis"),

        # SPECIAL MARKETS
        ("Player Double Double", "Anthony Davis"),
        ("Player Triple Double", "LeBron James"),
        ("Player To Have Most Points", "Kevin Durant"),
        ("Player To Have Most Assists", "Chris Paul"),

        # MLB MARKETS
        ("Player Hits Over 1.5", "Aaron Judge"),
        ("Player Home Runs Over 0.5", "Shohei Ohtani"),
        ("Player RBI Over 1.5", "Mike Trout"),
        ("Player Stolen Bases Over 0.5", "Trea Turner"),
        ("Player Strikeouts Over 5.5", "Jacob deGrom"),

        # WNBA MARKETS
        ("Player Points Over 20.5", "A'ja Wilson"),
        ("Player Rebounds Over 10.5", "Breanna Stewart"),
        ("Player Assists Over 5.5", "Chelsea Gray"),

        # NCAAF MARKETS
        ("Player Passing Yards Over 275.5", "Caleb Williams"),
        ("Player Rushing Yards Over 100.5", "Blake Corum"),
        ("Player Receptions Over 5.5", "Marvin Harrison Jr"),

        # NCAAB MARKETS
        ("Player Points Over 25.5", "Zach Edey"),
        ("Player Rebounds Over 12.5", "Kyle Filipowski"),
        ("Player Assists Over 7.5", "Tyler Kolek"),
        ("Player Double Double", "Zach Edey"),

        # PERIOD MARKETS
        ("1st Quarter Moneyline", "Boston Celtics"),
        ("1st Half Total Points Over 110.5", "Lakers vs Warriors"),
        ("2nd Half Point Spread", "Miami Heat -3.5"),
        ("1st 3 Quarters Moneyline 3-way", "Knicks vs Bulls"),
        ("4th Quarter Excluding Overtime Total Points", "Suns vs Nuggets"),

        # FIRST/LAST SCORER MARKETS
        ("Player First Basket", "Jayson Tatum"),
        ("Player First Field Goal", "Stephen Curry"),
        ("Player First Touchdown Scorer", "Christian McCaffrey"),
        ("Player Last Touchdown Scorer", "Patrick Mahomes"),
        ("Team Player First Field Goal", "Golden State Warriors"),

        # TEAM MARKETS
        ("Team Total Points Over 115.5", "Boston Celtics"),
        ("Team 1st Half Total Points Under 55.5", "Lakers"),
        ("Team Win Both Halves", "Milwaukee Bucks"),
        ("Team Win Every Quarter", "Denver Nuggets"),
        ("Team To Have Highest Three-Point Field Goal Percentage", "Warriors"),

        # GAME SPECIALS
        ("First Minute Both Teams To Score", "Yes"),
        ("Highest Scoring Quarter 5-way", "4th Quarter"),
        ("Lowest Scoring Quarter 5-way", "1st Quarter"),
        ("Largest Comeback", "Over 15.5"),
        ("Will There Be Overtime", "Yes"),

        # GAME BASIC MARKETS
        ("Moneyline", "Boston Celtics"),
        ("Point Spread -7.5", "Golden State Warriors"),
        ("Total Points Over 225.5", "Lakers vs Celtics"),
        ("Total Points Odd/Even", "Odd"),
    ]

    print("=" * 100)
    print("COMPLETE MARKET CLASSIFIER TEST - ALL MARKETS INCLUDING NEW SPORTS")
    print("=" * 100)

    for market, player in test_cases:
        print(f"\nTest: '{market}' | Player/Team: '{player}'")
        print("-" * 60)

        try:
            result = classifier.classify(market, player)
            print(f"  Category: {result.category}")
            print(f"  Subcategory: {result.subcategory}")
            print(f"  Clean Player: '{result.clean_player}'")
            print(f"  Clean Team: '{result.clean_team}'")
            print(f"  Direction: {result.direction}")
            print(f"  Line: {result.line_value}")
            print(f"  Period: {result.period}")
            print(f"  Stat Type: {result.stat_type}")
            print(f"  Resolution Type: '{classifier.get_resolution_type(result)}'")
            print(f"  Market for R: '{classifier.get_market_for_r_script(result)}'")
            print(f"  Confidence: {result.confidence}")

            if result.category == 'player_prop' and result.subcategory == 'generic':
                print(f"  ⚠️ GENERIC (PROBLEM!)")
            else:
                print(f"  ✓ CORRECTLY CLASSIFIED")

        except Exception as e:
            print(f"  ERROR: {e}")


if __name__ == "__main__":
    test_market_classifier()