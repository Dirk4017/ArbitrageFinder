#!/usr/bin/env Rscript
# Set up better error handling
options(error = function() {
  traceback(3)
  quit(status = 1)
})

# ========== CRITICAL: Redirect ALL debug output to stderr ==========
# Close any existing sinks
while (sink.number() > 0) sink()

# Redirect ALL output and messages to stderr
sink(stderr(), type = "output")
sink(stderr(), type = "message")

# Suppress package warnings
options(warn = -1)

# Null coalescing operator
`%||%` <- function(a, b) if (!is.null(a)) a else b
# Helper to safely extract values from nested ESPN structures
safe_extract <- function(obj, path) {
  tryCatch({
    if (is.null(obj)) return(NULL)
    
    parts <- strsplit(path, "\\$")[[1]]
    current <- obj
    
    for (part in parts) {
      if (is.null(current)) return(NULL)
      
      if (is.data.frame(current)) {
        if (part %in% colnames(current)) {
          current <- current[[part]]
        } else {
          return(NULL)
        }
      } else if (is.list(current)) {
        if (part %in% names(current)) {
          current <- current[[part]]
        } else {
          return(NULL)
        }
      } else {
        return(NULL)
      }
    }
    
    return(current)
  }, error = function(e) {
    return(NULL)
  })
}

cat("=== SCRIPT STARTED ===\n", file = stderr())

# NFL/NBA BET RESOLUTION SCRIPT - UPDATED FOR JANUARY 2026 RESOLVING DECEMBER 2025 GAMES
# Save as: bet_resolver.R
# Usage from Python: Rscript bet_resolver.R "player_name" "sport" "season" "market_type" "event_string" "line_value" "game_date"

library(nflreadr)
library(hoopR)
library(jsonlite)
library(stringr)
library(dplyr)
library(lubridate)
library(httr)  # Added for ESPN API

# Custom cat function that always writes to stderr
debug_cat <- function(...) {
  cat(..., file = stderr())
}

debug_print <- function(x) {
  print(x, file = stderr())
}

# ==============================================
# MLB STAT MAPPING - Ensures correct stat is extracted
# ==============================================
get_correct_player_stat <- function(stats_data, requested_stat, sport = "mlb") {
  if (is.null(stats_data) || is.null(stats_data$stats)) return(NA)

  if (sport != "mlb" && sport != "baseball") {
    val <- stats_data$stats[[requested_stat]]
    if (!is.null(val) && !is.na(val)) return(val)
    return(NA)
  }

  mlb_mapping <- list(
    home_runs = c("home_runs", "hr", "homers", "home_run"),
    runs = c("runs", "runs_scored", "r"),
    rbi = c("rbi", "runs_batted_in", "rbis"),
    stolen_bases = c("stolen_bases", "sb", "steals", "stolen_base"),
    walks = c("walks", "base_on_balls", "bb", "batting_walks"),
    batting_walks = c("walks", "base_on_balls", "bb", "batting_walks"),
    strikeouts = c("strikeouts", "so", "batting_strikeouts"),
    pitching_strikeouts = c("pitching_strikeouts", "strikeouts_pitched", "so_pitched"),
    hits = c("hits", "h"),
    total_bases = c("total_bases", "tb"),
    doubles = c("doubles", "2b"),
    triples = c("triples", "3b"),
    singles = c("singles", "1b"),
    at_bats = c("at_bats", "ab"),
    innings_pitched = c("innings_pitched", "ip"),
    earned_runs = c("earned_runs", "er", "earned_runs_allowed"),
    hits_allowed = c("hits_allowed", "ha"),
    walks_allowed = c("walks_allowed", "bb_allowed"),
    outs_recorded = c("outs_recorded", "outs"),
    home_runs_allowed = c("home_runs_allowed", "hra")
  )

  possible_fields <- mlb_mapping[[requested_stat]]
  if (is.null(possible_fields)) {
    possible_fields <- c(requested_stat)
  }

  for (field in possible_fields) {
    val <- stats_data$stats[[field]]
    if (!is.null(val) && !is.na(val) && is.numeric(val)) return(val)
  }

  return(NA)
}

# ==============================================
# MLB TEAM DETECTION - Override sport if MLB teams found in event
# ==============================================
check_mlb_team_in_event <- function(event_string) {
  event_lower <- tolower(event_string)
  mlb_teams <- c(
    "yankees", "red sox", "dodgers", "giants", "cubs", "white sox",
    "rangers", "athletics", "rays", "orioles", "blue jays", "twins",
    "guardians", "tigers", "royals", "astros", "angels", "mariners",
    "braves", "marlins", "mets", "phillies", "nationals", "padres",
    "rockies", "diamondbacks", "d-backs", "cardinals", "brewers", "pirates", "reds",
    "arizona diamondbacks", "atlanta braves", "baltimore orioles",
    "boston red sox", "chicago cubs", "chicago white sox",
    "cincinnati reds", "cleveland guardians", "colorado rockies",
    "detroit tigers", "houston astros", "kansas city royals",
    "los angeles angels", "los angeles dodgers", "miami marlins",
    "milwaukee brewers", "minnesota twins", "new york mets",
    "new york yankees", "oakland athletics", "philadelphia phillies",
    "pittsburgh pirates", "san diego padres", "san francisco giants",
    "seattle mariners", "st. louis cardinals", "st louis cardinals",
    "tampa bay rays", "texas rangers", "toronto blue jays",
    "washington nationals"
  )
  for (team in mlb_teams) {
    if (grepl(team, event_lower, fixed = TRUE)) return(TRUE)
  }
  return(FALSE)
}

# ==============================================
# UPDATED SPORT NORMALIZATION
# ==============================================
normalize_sport_name <- function(sport) {
  sport_lower <- tolower(trimws(sport))
  
  # NFL
  if (sport_lower %in% c("football", "nfl", "nfl football", "american football")) {
    return("nfl")
  }
  # NBA
  else if (sport_lower %in% c("basketball", "nba", "nba basketball")) {
    return("nba")
  }
  # NHL
  else if (sport_lower %in% c("hockey", "nhl", "ice hockey", "nhl hockey")) {
    return("nhl")
  }
  # MLB
  else if (sport_lower %in% c("baseball", "mlb", "mlb baseball")) {
    return("mlb")
  }
  # WNBA
  else if (sport_lower %in% c("wnba", "womens basketball", "women's basketball")) {
    return("wnba")
  }
  # NCAA Football
  else if (sport_lower %in% c("ncaaf", "college football", "ncaa football", "cfb")) {
    return("ncaaf")
  }
  # NCAA Women's Basketball
  else if (sport_lower %in% c("ncaaw", "ncaa women's basketball", "college women's basketball", "wcbb")) {
    return("ncaaw")
  }
  # NCAA Men's Basketball
  else if (sport_lower %in% c("ncaab", "ncaa basketball", "college basketball", "ncaa men's basketball", "mcbb")) {
    return("ncaab")
  }
  else {
    return(sport_lower)
  }
}
# ==============================================
# NEW FUNCTIONS FOR ADDITIONAL MARKET TYPES
# ==============================================

# Helper function for classification
extract_stat_from_market <- function(market_lower) {
  if (grepl("points", market_lower)) return("points")
  if (grepl("rebounds", market_lower)) return("rebounds")
  if (grepl("assists", market_lower)) return("assists")
  if (grepl("steals", market_lower)) return("steals")
  if (grepl("blocks", market_lower)) return("blocks")
  if (grepl("turnovers", market_lower)) return("turnovers")
  if (grepl("threes|3pt|three.*point", market_lower)) return("three_pointers_made")  # ADDED THIS LINE
  if (grepl("passing.*yards", market_lower)) return("passing_yards")
  if (grepl("rushing.*yards", market_lower)) return("rushing_yards")
  if (grepl("receiving.*yards", market_lower)) return("receiving_yards")
  if (grepl("touchdowns", market_lower)) return("touchdowns")
  if (grepl("receptions", market_lower)) return("receptions")
  return("unknown")
}
#' Resolve game market (moneyline, spread, total)
resolve_game_market <- function(event_string, sport, season, market_type, line_value = NULL, direction = NULL, team = NULL, game_date = NULL) {
  
  debug_cat("\n=== RESOLVING GAME MARKET ===\n")
  debug_cat(sprintf("Event: %s\n", event_string))
  debug_cat(sprintf("Sport: %s\n", sport))
  debug_cat(sprintf("Market: %s\n", market_type))
  debug_cat(sprintf("Line: %s\n", line_value))
  debug_cat(sprintf("Direction: %s\n", direction))
  debug_cat(sprintf("Team: %s\n", team))
  
  # Replace this section in resolve_game_market:
  
  # Clean the event string - but be much less aggressive!
  event_clean <- event_string
  
  # Only remove common bet prefixes if they exist at the START of the string
  event_clean <- gsub("^Over\\s+\\d+\\.?\\d*\\s*-\\s*", "", event_clean, ignore.case = TRUE)
  event_clean <- gsub("^Under\\s+\\d+\\.?\\d*\\s*-\\s*", "", event_clean, ignore.case = TRUE)
  event_clean <- gsub("^\\+\\d+\\.?\\d*\\s*-\\s*", "", event_clean)
  event_clean <- gsub("^-\\d+\\.?\\d*\\s*-\\s*", "", event_clean)
  event_clean <- gsub("^Moneyline\\s*-\\s*", "", event_clean, ignore.case = TRUE)
  event_clean <- gsub("^Point Spread\\s*-\\s*", "", event_clean, ignore.case = TRUE)
  event_clean <- gsub("^Total Points\\s*-\\s*", "", event_clean, ignore.case = TRUE)
  
  # Remove date at the end ONLY if it's actually at the end
  event_clean <- gsub("\\s+\\d{4}-\\d{2}-\\d{2}$", "", event_clean)
  event_clean <- trimws(event_clean)
  
  debug_cat(sprintf("Cleaned event: '%s'\n", event_clean))
  
  # Extract teams
  teams <- extract_teams_from_event(event_clean, sport)
  if (is.null(teams)) {
    return(list(
      success = FALSE,
      resolved = FALSE,
      error = "Could not extract teams from event string",
      market = market_type,
      event = event_string
    ))
  }
  
  debug_cat(sprintf("Teams: %s @ %s\n", teams$away, teams$home))
  
  # Extract game date
  game_date_obj <- extract_date_from_event(event_string, game_date)
  debug_cat(sprintf("Game date: %s\n", game_date_obj))
  
  # Find game ID
  game_id <- find_game_id(teams$home, teams$away, game_date_obj, sport)
  if (is.na(game_id) || is.null(game_id)) {
    return(list(
      success = FALSE,
      resolved = FALSE,
      error = "Could not find game ID",
      home_team = teams$home,
      away_team = teams$away,
      game_date = as.character(game_date_obj)
    ))
  }
  
  debug_cat(sprintf("Found game ID: %s\n", game_id))
  
  # Get game result based on sport
  if (sport %in% c("nba", "basketball")) {
    return(resolve_nba_game(game_id, market_type, line_value, direction, team, teams))
  } else if (sport %in% c("nfl", "football")) {
    return(resolve_nfl_game(game_id, season, market_type, line_value, direction, team, teams))
  } else {
    return(list(
      success = FALSE,
      resolved = FALSE,
      error = paste("Sport not supported for game markets:", sport)
    ))
  }
}

resolve_nba_game <- function(game_id, market_type, line_value, direction, team, teams) {
  
  debug_cat("\n=== RESOLVING NBA GAME ===\n")
  
  # Try ESPN API first
  url <- paste0("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event=", game_id)
  data <- tryCatch({
    fromJSON(url)
  }, error = function(e) {
    debug_cat(sprintf("Error fetching ESPN data: %s\n", e$message))
    return(NULL)
  })
  
  if (is.null(data)) {
    return(list(
      success = FALSE,
      resolved = FALSE,
      error = "Could not fetch game data from ESPN",
      game_id = game_id
    ))
  }
  
  # Extract game result
  result <- list(
    success = TRUE,
    resolved = TRUE,
    game_id = game_id,
    market = market_type
  )
  
  # Get scores from header
  if (!is.null(data$header) && !is.null(data$header$competitions)) {
    comp <- data$header$competitions
    
    if (is.data.frame(comp) && nrow(comp) > 0) {
      # Extract competitors
      if (!is.null(comp$competitors) && length(comp$competitors) > 0) {
        competitors <- comp$competitors[[1]]
        
        if (is.data.frame(competitors) && nrow(competitors) >= 2) {
          # Find home and away teams
          home_row <- competitors[competitors$homeAway == "home", ]
          away_row <- competitors[competitors$homeAway == "away", ]
          
          if (nrow(home_row) > 0 && nrow(away_row) > 0) {
            result$home_team <- home_row$team$displayName %||% home_row$team.displayName
            result$away_team <- away_row$team$displayName %||% away_row$team.displayName
            result$home_score <- as.numeric(home_row$score %||% 0)
            result$away_score <- as.numeric(away_row$score %||% 0)
            result$total_points <- result$home_score + result$away_score
            
            debug_cat(sprintf("Final score: %s %d - %d %s\n", 
                              result$away_team, result$away_score,
                              result$home_score, result$home_team))
          }
        }
      }
    }
  }
  
  # If we couldn't get scores from header, try boxscore
  if (is.null(result$home_score) && !is.null(data$boxscore)) {
    if (!is.null(data$boxscore$teams)) {
      teams_df <- data$boxscore$teams
      if (is.data.frame(teams_df) && nrow(teams_df) >= 2) {
        for (i in 1:nrow(teams_df)) {
          team_info <- teams_df[i, ]
          if (!is.null(team_info$homeAway)) {
            if (team_info$homeAway == "home") {
              result$home_team <- team_info$team$displayName %||% team_info$team.displayName
              result$home_score <- as.numeric(team_info$score %||% 0)
            } else {
              result$away_team <- team_info$team$displayName %||% team_info$team.displayName
              result$away_score <- as.numeric(team_info$score %||% 0)
            }
          }
        }
        result$total_points <- (result$home_score %||% 0) + (result$away_score %||% 0)
      }
    }
  }
  
  # Check if we have scores
  if (is.null(result$home_score) || is.null(result$away_score)) {
    return(list(
      success = FALSE,
      resolved = FALSE,
      error = "Could not extract scores from game data",
      game_id = game_id
    ))
  }
  
  # Check if game is completed (scores are not 0)
  if (result$home_score == 0 && result$away_score == 0) {
    return(list(
      success = FALSE,
      resolved = FALSE,
      error = "Game not yet completed - scores not available",
      game_id = game_id,
      home_team = result$home_team,
      away_team = result$away_team,
      is_future = TRUE
    ))
  }
  
  # Determine which team was bet on
  bet_team <- NULL
  if (!is.null(team) && nchar(team) > 0) {
    # Try to match the bet team to home or away
    team_lower <- tolower(team)
    home_lower <- tolower(result$home_team)
    away_lower <- tolower(result$away_team)
    
    if (grepl(team_lower, home_lower) || grepl(home_lower, team_lower)) {
      bet_team <- "home"
    } else if (grepl(team_lower, away_lower) || grepl(away_lower, team_lower)) {
      bet_team <- "away"
    }
  }
  
  # If still no bet team, use the team parameter
  if (is.null(bet_team) && !is.null(team)) {
    bet_team <- team
  }
  
  # Calculate result based on market type
  market_lower <- tolower(market_type)
  
  if (grepl("moneyline", market_lower)) {
    # Moneyline - which team won?
    if (!is.null(bet_team)) {
      if (bet_team == "home") {
        result$won <- result$home_score > result$away_score
      } else if (bet_team == "away") {
        result$won <- result$away_score > result$home_score
      } else {
        result$won <- FALSE
      }
    } else {
      # No team specified - return the winner
      if (result$home_score > result$away_score) {
        result$winner <- "home"
        result$winner_team <- result$home_team
      } else if (result$away_score > result$home_score) {
        result$winner <- "away"
        result$winner_team <- result$away_team
      } else {
        result$winner <- "tie"
      }
    }
    result$bet_type <- "moneyline"
    
  } else if (grepl("spread|point.*spread", market_lower)) {
    # Point spread - need line value
    if (!is.null(line_value)) {
      line_num <- as.numeric(line_value)
      
      # SAFETY CHECK: Ensure scores are valid for calculation
      if (is.na(result$home_score) || is.na(result$away_score)) {
        return(list(
          success = FALSE,
          resolved = FALSE,
          error = "Invalid scores for spread calculation",
          game_id = game_id
        ))
      }
      
      # Calculate point differential
      point_diff <- result$home_score - result$away_score
      
      if (!is.null(bet_team)) {
        # Team was specified and matched
        if (bet_team == "home") {
          # Home team + spread
          adjusted_score <- result$home_score + line_num
          if (is.na(adjusted_score) || is.na(result$away_score)) {
            result$won <- FALSE
          } else {
            result$won <- adjusted_score > result$away_score
          }
          debug_cat(sprintf("Home spread: %s + %s = %s vs Away %s = %s\n",
                            result$home_score, line_num, adjusted_score,
                            result$away_score, result$won))
        } else if (bet_team == "away") {
          # Away team + spread
          adjusted_score <- result$away_score + line_num
          if (is.na(adjusted_score) || is.na(result$home_score)) {
            result$won <- FALSE
          } else {
            result$won <- adjusted_score > result$home_score
          }
          debug_cat(sprintf("Away spread: %s + %s = %s vs Home %s = %s\n",
                            result$away_score, line_num, adjusted_score,
                            result$home_score, result$won))
        } else {
          result$won <- FALSE
        }
      } else {
        # No team specified - interpret the line value
        # Negative line means home team is favored (e.g., -5.5 means home -5.5)
        # Positive line means away team is favored (e.g., +5.5 means away +5.5)
        result$point_differential <- point_diff
        result$line_value <- line_num
        
        if (line_num < 0) {
          # Home team is favored by abs(line_num)
          # Bet wins if home team wins by more than the spread
          result$won <- point_diff > abs(line_num)
          debug_cat(sprintf("Spread (home favored %s): %s > %s = %s\n", 
                            line_num, point_diff, abs(line_num), result$won))
        } else if (line_num > 0) {
          # Away team is favored by line_num
          # Bet wins if away team wins by more than the spread
          # Which is equivalent to home team loses by more than the spread
          result$won <- -point_diff > line_num
          debug_cat(sprintf("Spread (away favored +%s): %s > %s = %s\n", 
                            line_num, -point_diff, line_num, result$won))
        } else {
          # Pick'em (line = 0) - bet wins if team wins outright
          # Without a specified team, this is ambiguous
          result$won <- FALSE
          result$warning <- "Line is 0 but no team specified - cannot determine winner"
        }
      }
      result$line_value <- line_num
    } else {
      return(list(
        success = FALSE,
        resolved = FALSE,
        error = "Line value required for spread bets"
      ))
    }
    result$bet_type <- "spread"
    
  } else if (grepl("total", market_lower)) {
    # Total points - need line value and direction
    if (!is.null(line_value) && !is.null(direction)) {
      line_num <- as.numeric(line_value)
      result$actual_value <- result$total_points
      result$line_value <- line_num
      
      # SAFETY CHECK: Ensure total points is valid
      if (is.na(result$total_points)) {
        result$won <- FALSE
      } else {
        if (tolower(direction) == "over") {
          result$won <- result$total_points > line_num
        } else if (tolower(direction) == "under") {
          result$won <- result$total_points < line_num
        } else {
          result$won <- FALSE
        }
      }
      debug_cat(sprintf("Total %s: %s %s %s = %s\n",
                        direction, result$total_points,
                        ifelse(direction == "over", ">", "<"),
                        line_num, result$won))
    } else {
      return(list(
        success = FALSE,
        resolved = FALSE,
        error = "Line value and direction required for total bets"
      ))
    }
    result$bet_type <- "total"
  }
  
  return(result)
}

#' Resolve NFL game
resolve_nfl_game <- function(game_id, season, market_type, line_value, direction, team, teams) {
  
  debug_cat("\n=== RESOLVING NFL GAME ===\n")
  
  # Load NFL schedule to get game info
  schedule <- tryCatch({
    nflreadr::load_schedules(season)
  }, error = function(e) {
    debug_cat(sprintf("Error loading NFL schedule: %s\n", e$message))
    return(NULL)
  })
  
  if (is.null(schedule) || nrow(schedule) == 0) {
    return(list(
      success = FALSE,
      resolved = FALSE,
      error = "Could not load NFL schedule"
    ))
  }
  
  # Find the game
  game <- schedule[schedule$game_id == game_id, ]
  if (nrow(game) == 0) {
    return(list(
      success = FALSE,
      resolved = FALSE,
      error = paste("Game not found in schedule:", game_id)
    ))
  }
  
  # Get scores
  result <- list(
    success = TRUE,
    resolved = TRUE,
    game_id = game_id,
    home_team = game$home_team,
    away_team = game$away_team,
    home_score = as.numeric(game$home_score %||% 0),
    away_score = as.numeric(game$away_score %||% 0),
    season = season,
    market = market_type
  )
  
  result$total_points <- result$home_score + result$away_score
  
  # Check if game is completed
  if (is.na(result$home_score) || is.na(result$away_score) || 
      (result$home_score == 0 && result$away_score == 0)) {
    return(list(
      success = FALSE,
      resolved = FALSE,
      error = "Game not completed or scores not available",
      game_id = game_id,
      week = game$week
    ))
  }
  
  # Determine which team was bet on
  bet_team <- NULL
  if (!is.null(team) && nchar(team) > 0) {
    team_std <- standardize_team_name(team, "nfl")
    if (!is.na(team_std)) {
      if (team_std == result$home_team) {
        bet_team <- "home"
      } else if (team_std == result$away_team) {
        bet_team <- "away"
      }
    }
  }
  
  # Calculate result based on market type
  market_lower <- tolower(market_type)
  
  if (grepl("moneyline", market_lower)) {
    if (!is.null(bet_team)) {
      if (bet_team == "home") {
        result$won <- result$home_score > result$away_score
      } else if (bet_team == "away") {
        result$won <- result$away_score > result$home_score
      } else {
        result$won <- FALSE
      }
    } else {
      if (result$home_score > result$away_score) {
        result$winner <- "home"
        result$winner_team <- result$home_team
      } else if (result$away_score > result$home_score) {
        result$winner <- "away"
        result$winner_team <- result$away_team
      } else {
        result$winner <- "tie"
      }
    }
    result$bet_type <- "moneyline"
    
  } else if (grepl("spread|point.*spread", market_lower)) {
    if (!is.null(line_value)) {
      line_num <- as.numeric(line_value)
      
      if (!is.null(bet_team)) {
        if (bet_team == "home") {
          adjusted_score <- result$home_score + line_num
          result$won <- adjusted_score > result$away_score
        } else if (bet_team == "away") {
          adjusted_score <- result$away_score + line_num
          result$won <- adjusted_score > result$home_score
        } else {
          result$won <- FALSE
        }
      } else {
        spread_result <- result$home_score - result$away_score
        result$spread <- spread_result
        result$won <- spread_result > line_num
      }
      result$line_value <- line_num
    } else {
      return(list(
        success = FALSE,
        resolved = FALSE,
        error = "Line value required for spread bets"
      ))
    }
    result$bet_type <- "spread"
    
  } else if (grepl("total", market_lower)) {
    if (!is.null(line_value) && !is.null(direction)) {
      line_num <- as.numeric(line_value)
      result$actual_value <- result$total_points
      result$line_value <- line_num
      
      if (tolower(direction) == "over") {
        result$won <- result$total_points > line_num
      } else if (tolower(direction) == "under") {
        result$won <- result$total_points < line_num
      } else {
        result$won <- FALSE
      }
    } else {
      return(list(
        success = FALSE,
        resolved = FALSE,
        error = "Line value and direction required for total bets"
      ))
    }
    result$bet_type <- "total"
  }
  
  return(result)
}
# ==============================================
# SPECIAL MARKETS RESOLVER - NEW FUNCTION
# ==============================================

resolve_special_prop <- function(game_id, player_name, sport, season, special_type, line_value = NULL) {
  debug_cat(sprintf("\nResolving special prop: %s for %s\n", special_type, player_name))
  
  tryCatch({
    if (sport %in% c("nba", "basketball")) {
      # Use your existing ESPN function
      stats <- get_espn_nba_player_stats(game_id, player_name)
      
      if (is.null(stats)) {
        return(list(success = FALSE, error = "Could not fetch player stats"))
      }
      
      result <- list(
        success = TRUE,
        special_type = special_type,
        stats = stats  # Include all stats for debugging
      )
      
      # Determine outcome based on special type
      if (special_type == "double_double") {
        # Count categories where player had >= 10
        categories <- 0
        categories_list <- list()
        
        if (stats$points >= 10) {
          categories <- categories + 1
          categories_list$points <- stats$points
        }
        if (stats$rebounds >= 10) {
          categories <- categories + 1
          categories_list$rebounds <- stats$rebounds
        }
        if (stats$assists >= 10) {
          categories <- categories + 1
          categories_list$assists <- stats$assists
        }
        if (stats$blocks >= 10) {
          categories <- categories + 1
          categories_list$blocks <- stats$blocks
        }
        if (stats$steals >= 10) {
          categories <- categories + 1
          categories_list$steals <- stats$steals
        }
        
        result$achieved <- categories >= 2
        result$categories <- categories
        result$categories_list <- categories_list
        result$details <- list(
          points = stats$points,
          rebounds = stats$rebounds,
          assists = stats$assists,
          blocks = stats$blocks,
          steals = stats$steals
        )
        
        debug_cat(sprintf("  Double Double: Categories >= 10: %s, Achieved: %s\n",
                          categories, result$achieved))
        
      } else if (special_type == "triple_double") {
        # Count categories where player had >= 10
        categories <- 0
        categories_list <- list()
        
        if (stats$points >= 10) {
          categories <- categories + 1
          categories_list$points <- stats$points
        }
        if (stats$rebounds >= 10) {
          categories <- categories + 1
          categories_list$rebounds <- stats$rebounds
        }
        if (stats$assists >= 10) {
          categories <- categories + 1
          categories_list$assists <- stats$assists
        }
        if (stats$blocks >= 10) {
          categories <- categories + 1
          categories_list$blocks <- stats$blocks
        }
        if (stats$steals >= 10) {
          categories <- categories + 1
          categories_list$steals <- stats$steals
        }
        
        result$achieved <- categories >= 3
        result$categories <- categories
        result$categories_list <- categories_list
        result$details <- list(
          points = stats$points,
          rebounds = stats$rebounds,
          assists = stats$assists,
          blocks = stats$blocks,
          steals = stats$steals
        )
        
        debug_cat(sprintf("  Triple Double: Categories >= 10: %s, Achieved: %s\n",
                          categories, result$achieved))
        
      } else if (special_type == "three_pointers_made") {
        # Just return the three-pointers made
        result$actual_value <- stats$three_pointers_made
        result$stat_name <- "three_pointers_made"
        debug_cat(sprintf("  Three-pointers made: %s\n", result$actual_value))
        
      } else if (grepl("most_", special_type)) {
        # For "most points/rebounds/assists" - we need to check all players in the game
        debug_cat("  WARNING: Most points/rebounds/assists markets require comparing all players\n")
        
        if (special_type == "most_points") {
          result$actual_value <- stats$points
          result$stat_name <- "points"
          result$warning <- "Most points market requires comparing all players - returning player's points only"
          debug_cat(sprintf("  Player points: %s\n", result$actual_value))
        } else if (special_type == "most_rebounds") {
          result$actual_value <- stats$rebounds
          result$stat_name <- "rebounds"
          result$warning <- "Most rebounds market requires comparing all players - returning player's rebounds only"
          debug_cat(sprintf("  Player rebounds: %s\n", result$actual_value))
        } else if (special_type == "most_assists") {
          result$actual_value <- stats$assists
          result$stat_name <- "assists"
          result$warning <- "Most assists market requires comparing all players - returning player's assists only"
          debug_cat(sprintf("  Player assists: %s\n", result$actual_value))
        }
      }
      
      if (!is.null(line_value)) {
        result$line_value <- as.numeric(line_value)
        debug_cat(sprintf("  Line value: %s\n", result$line_value))
      }
      
      return(result)
      
    } else {
      return(list(success = FALSE, error = paste("Special props not implemented for:", sport)))
    }
    
  }, error = function(e) {
    return(list(success = FALSE, error = paste("Error in resolve_special_prop:", e$message)))
  })
}

# ==============================================
# NHL MARKET CLASSIFICATION
# ==============================================

classify_nhl_market <- function(market_lower) {
  debug_cat("  Classifying as NHL market...\n")
  
  # ==================== PERIOD MARKETS ====================
  # Period moneyline
  if (grepl("1st.*period.*moneyline", market_lower)) {
    if (grepl("3.way", market_lower) || grepl("3way", market_lower)) {
      return(list(type = "period_moneyline", period = "1st", bet_type = "3way"))
    }
    return(list(type = "period_moneyline", period = "1st", bet_type = "2way"))
  }
  if (grepl("2nd.*period.*moneyline", market_lower)) {
    if (grepl("3.way", market_lower) || grepl("3way", market_lower)) {
      return(list(type = "period_moneyline", period = "2nd", bet_type = "3way"))
    }
    return(list(type = "period_moneyline", period = "2nd", bet_type = "2way"))
  }
  if (grepl("3rd.*period.*moneyline", market_lower)) {
    if (grepl("3.way", market_lower) || grepl("3way", market_lower)) {
      return(list(type = "period_moneyline", period = "3rd", bet_type = "3way"))
    }
    return(list(type = "period_moneyline", period = "3rd", bet_type = "2way"))
  }
  
  # Period puck line (spread)
  if (grepl("1st.*period.*puck.*line", market_lower)) {
    return(list(type = "period_puckline", period = "1st"))
  }
  if (grepl("2nd.*period.*puck.*line", market_lower)) {
    return(list(type = "period_puckline", period = "2nd"))
  }
  if (grepl("3rd.*period.*puck.*line", market_lower)) {
    return(list(type = "period_puckline", period = "3rd"))
  }
  
  # Period total goals
  if (grepl("1st.*period.*total.*goals", market_lower)) {
    if (grepl("odd.*even", market_lower)) {
      return(list(type = "period_total_goals_oddeven", period = "1st"))
    }
    return(list(type = "period_total_goals", period = "1st"))
  }
  if (grepl("2nd.*period.*total.*goals", market_lower)) {
    if (grepl("odd.*even", market_lower)) {
      return(list(type = "period_total_goals_oddeven", period = "2nd"))
    }
    return(list(type = "period_total_goals", period = "2nd"))
  }
  if (grepl("3rd.*period.*total.*goals", market_lower)) {
    if (grepl("odd.*even", market_lower)) {
      return(list(type = "period_total_goals_oddeven", period = "3rd"))
    }
    return(list(type = "period_total_goals", period = "3rd"))
  }
  
  # Period both teams to score
  if (grepl("1st.*period.*both.*teams.*to.*score", market_lower)) {
    return(list(type = "period_both_teams_score", period = "1st"))
  }
  if (grepl("2nd.*period.*both.*teams.*to.*score", market_lower)) {
    return(list(type = "period_both_teams_score", period = "2nd"))
  }
  if (grepl("3rd.*period.*both.*teams.*to.*score", market_lower)) {
    return(list(type = "period_both_teams_score", period = "3rd"))
  }
  
  # First X minutes total goals
  if (grepl("first.*10.*minutes.*total.*goals", market_lower)) {
    return(list(type = "minutes_total_goals", minutes = 10))
  }
  if (grepl("first.*5.*minutes.*total.*goals", market_lower)) {
    return(list(type = "minutes_total_goals", minutes = 5))
  }
  
  # ==================== SPECIAL PLAYER MARKETS (MUST BE BEFORE PLAYER MARKETS) ====================
  # Player to record a shutout
  if (grepl("player.*record.*shutout", market_lower) || grepl("player.*shutout", market_lower)) {
    return(list(type = "player_special", special_type = "shutout"))
  }
  
  # Player to have most goals
  if (grepl("player.*to.*have.*most.*goals", market_lower)) {
    return(list(type = "player_special", special_type = "most_goals"))
  }
  
  # Player to have most points
  if (grepl("player.*to.*have.*most.*points", market_lower)) {
    return(list(type = "player_special", special_type = "most_points"))
  }
  
  # Player to have most shots on goal
  if (grepl("player.*to.*have.*most.*shots.*on.*goal", market_lower)) {
    return(list(type = "player_special", special_type = "most_shots"))
  }
  
  # ==================== PLAYER PERIOD MARKETS ====================
  # Player period goals
  if (grepl("player.*1st.*period.*goals", market_lower)) {
    return(list(type = "player_period_stat", stat = "goals", period = "1st"))
  }
  if (grepl("player.*2nd.*period.*goals", market_lower)) {
    return(list(type = "player_period_stat", stat = "goals", period = "2nd"))
  }
  if (grepl("player.*3rd.*period.*goals", market_lower)) {
    return(list(type = "player_period_stat", stat = "goals", period = "3rd"))
  }
  
  # Player period assists
  if (grepl("player.*1st.*period.*assists", market_lower)) {
    return(list(type = "player_period_stat", stat = "assists", period = "1st"))
  }
  if (grepl("player.*2nd.*period.*assists", market_lower)) {
    return(list(type = "player_period_stat", stat = "assists", period = "2nd"))
  }
  if (grepl("player.*3rd.*period.*assists", market_lower)) {
    return(list(type = "player_period_stat", stat = "assists", period = "3rd"))
  }
  
  # Player period points
  if (grepl("player.*1st.*period.*points", market_lower)) {
    return(list(type = "player_period_stat", stat = "points", period = "1st"))
  }
  if (grepl("player.*2nd.*period.*points", market_lower)) {
    return(list(type = "player_period_stat", stat = "points", period = "2nd"))
  }
  
  # Player period shots on goal
  if (grepl("player.*1st.*period.*shots.*on.*goal", market_lower)) {
    return(list(type = "player_period_stat", stat = "shots", period = "1st"))
  }
  if (grepl("player.*2nd.*period.*shots.*on.*goal", market_lower)) {
    return(list(type = "player_period_stat", stat = "shots", period = "2nd"))
  }
  if (grepl("player.*3rd.*period.*shots.*on.*goal", market_lower)) {
    return(list(type = "player_period_stat", stat = "shots", period = "3rd"))
  }
  
  # Player period saves
  if (grepl("player.*1st.*period.*saves", market_lower)) {
    return(list(type = "player_period_stat", stat = "saves", period = "1st"))
  }
  if (grepl("player.*2nd.*period.*saves", market_lower)) {
    return(list(type = "player_period_stat", stat = "saves", period = "2nd"))
  }
  if (grepl("player.*3rd.*period.*saves", market_lower)) {
    return(list(type = "player_period_stat", stat = "saves", period = "3rd"))
  }
  
  # ==================== PLAYER MARKETS ====================
  # Player goals (exclude "most" markets)
  if (grepl("player.*goals", market_lower) && !grepl("against", market_lower) && !grepl("most", market_lower)) {
    return(list(type = "player_stat", stat = "goals"))
  }
  
  # Player assists
  if (grepl("player.*assists", market_lower)) {
    return(list(type = "player_stat", stat = "assists"))
  }
  
  # Player points
  if (grepl("player.*points", market_lower)) {
    return(list(type = "player_stat", stat = "points"))
  }
  
  # Player shots on goal
  if (grepl("player.*shots.*on.*goal", market_lower)) {
    return(list(type = "player_stat", stat = "shots"))
  }
  
  # Player saves
  if (grepl("player.*saves", market_lower)) {
    return(list(type = "player_stat", stat = "saves"))
  }
  
  # Player goals against (goalie)
  if (grepl("player.*goals.*against", market_lower)) {
    return(list(type = "player_stat", stat = "goals_against"))
  }
  
  # Player hits
  if (grepl("player.*hits", market_lower)) {
    return(list(type = "player_stat", stat = "hits"))
  }
  
  # Player blocked shots
  if (grepl("player.*blocked.*shots", market_lower) || grepl("player.*blocks", market_lower)) {
    return(list(type = "player_stat", stat = "blocked_shots"))
  }
  
  # Player faceoffs won
  if (grepl("player.*faceoffs.*won", market_lower)) {
    return(list(type = "player_stat", stat = "faceoffs_won"))
  }
  
  # Player plus/minus
  if (grepl("player.*plus.*minus", market_lower) || grepl("player.*plus/minus", market_lower) || grepl("player.*\\+/-", market_lower)) {
    return(list(type = "player_stat", stat = "plus_minus"))
  }
  
  # Player penalty minutes
  if (grepl("penalty minutes|pim", market_lower)) {
    return(list(type = "player_stat", stat = "penalty_minutes"))
  }
  
  # Player time on ice
  if (grepl("time on ice|toi", market_lower)) {
    return(list(type = "player_stat", stat = "time_on_ice"))
  }
  
  # Player power play points
  if (grepl("player.*power.*play.*points", market_lower)) {
    return(list(type = "player_stat", stat = "power_play_points"))
  }
  
  # ==================== SCORER MARKETS ====================
  # Player first goal
  if (grepl("player.*first.*goal", market_lower)) {
    return(list(type = "first_scorer", scorer_type = "goal"))
  }
  
  # Player last goal
  if (grepl("player.*last.*goal", market_lower)) {
    return(list(type = "last_scorer", scorer_type = "goal"))
  }
  
  # Team player first goal scorer
  if (grepl("team.*player.*first.*goal", market_lower)) {
    return(list(type = "team_first_scorer"))
  }
  
  # ==================== TEAM MARKETS ====================
  # Team total goals
  if (grepl("team.*total.*goals", market_lower)) {
    if (grepl("odd.*even", market_lower)) {
      return(list(type = "team_total_goals_oddeven"))
    }
    return(list(type = "team_total_goals"))
  }
  
  # Team period total goals
  if (grepl("team.*1st.*period.*total.*goals", market_lower)) {
    return(list(type = "team_period_total_goals", period = "1st"))
  }
  if (grepl("team.*2nd.*period.*total.*goals", market_lower)) {
    return(list(type = "team_period_total_goals", period = "2nd"))
  }
  if (grepl("team.*3rd.*period.*total.*goals", market_lower)) {
    return(list(type = "team_period_total_goals", period = "3rd"))
  }
  
  # Team total shots on goal
  if (grepl("team.*total.*shots.*on.*goal", market_lower)) {
    return(list(type = "team_total_shots"))
  }
  
  # Team total power play goals
  if (grepl("team.*total.*power.*play.*goals", market_lower)) {
    return(list(type = "team_power_play_goals"))
  }
  
  # Team total shorthanded goals
  if (grepl("team.*total.*shorthanded.*goals", market_lower)) {
    return(list(type = "team_shorthanded_goals"))
  }
  
  # Team to score last
  if (grepl("team.*to.*score.*last", market_lower) && !grepl("regulation", market_lower)) {
    return(list(type = "team_score_last"))
  }
  
  # ==================== REGULATION MARKETS (MOVED BEFORE GAME MARKETS) ====================
  if (grepl("regulation", market_lower)) {
    # Regulation moneyline
    if (grepl("moneyline", market_lower)) {
      if (grepl("3.way", market_lower) || grepl("3way", market_lower)) {
        return(list(type = "regulation_moneyline", bet_type = "3way"))
      }
      return(list(type = "regulation_moneyline", bet_type = "2way"))
    }
    
    # Regulation puck line
    if (grepl("puck.*line", market_lower)) {
      return(list(type = "regulation_puck_line"))
    }
    
    # Regulation total goals
    if (grepl("total.*goals", market_lower)) {
      return(list(type = "regulation_total_goals"))
    }
    
    # Regulation both teams to score
    if (grepl("both.*teams.*to.*score", market_lower)) {
      return(list(type = "regulation_both_teams_score"))
    }
    
    # Regulation team to score first
    if (grepl("team.*to.*score.*first", market_lower)) {
      if (grepl("3.way", market_lower) || grepl("3way", market_lower)) {
        return(list(type = "regulation_team_score_first", bet_type = "3way"))
      }
      return(list(type = "regulation_team_score_first", bet_type = "2way"))
    }
    
    # Regulation team to score last
    if (grepl("team.*to.*score.*last", market_lower)) {
      if (grepl("3.way", market_lower) || grepl("3way", market_lower)) {
        return(list(type = "regulation_team_score_last", bet_type = "3way"))
      }
      return(list(type = "regulation_team_score_last", bet_type = "2way"))
    }
    
    # Regulation shutout
    if (grepl("shutout", market_lower)) {
      return(list(type = "regulation_shutout"))
    }
  }
  
  # ==================== GAME MARKETS ====================
  # SPECIFIC TOTAL MARKETS FIRST (before generic total goals)
  if (grepl("total power play goals", market_lower)) {
    return(list(type = "game_power_play_goals"))
  }
  if (grepl("total shots on goal", market_lower)) {
    return(list(type = "game_total_shots"))
  }
  if (grepl("total goals odd/even", market_lower)) {
    return(list(type = "game_total_goals_oddeven"))
  }
  
  # Moneyline
  if (grepl("moneyline", market_lower)) {
    if (grepl("3.way", market_lower) || grepl("3way", market_lower)) {
      return(list(type = "game_moneyline", bet_type = "3way"))
    }
    return(list(type = "game_moneyline", bet_type = "2way"))
  }
  
  # Puck line (spread)
  if (grepl("puck.*line", market_lower)) {
    return(list(type = "game_puckline"))
  }
  
  # Both teams to score
  if (grepl("both.*teams.*to.*score", market_lower) && !grepl("period", market_lower)) {
    return(list(type = "game_both_teams_score"))
  }
  
  # Will there be overtime
  if (grepl("will.*there.*be.*overtime", market_lower)) {
    return(list(type = "game_overtime"))
  }
  
  # Goal in each period
  if (grepl("goal.*in.*each.*period", market_lower)) {
    return(list(type = "goal_each_period"))
  }
  
  # Highest scoring period
  if (grepl("highest.*scoring.*period", market_lower)) {
    return(list(type = "highest_scoring_period", num_options = 4))
  }
  
  # Generic total goals (LAST - only if no specific pattern matched)
  if (grepl("total.*goals", market_lower) && !grepl("period|first|power|shots", market_lower)) {
    return(list(type = "game_total_goals"))
  }
  
  # ==================== TEAM PROP MARKETS ====================
  # Team to score first
  if (grepl("team.*to.*score.*first", market_lower) && !grepl("regulation", market_lower)) {
    if (grepl("3.way", market_lower) || grepl("3way", market_lower)) {
      return(list(type = "team_score_first", bet_type = "3way"))
    }
    return(list(type = "team_score_first", bet_type = "2way"))
  }
  
  # Team score first and win
  if (grepl("team.*score.*first.*and.*win", market_lower)) {
    return(list(type = "team_score_first_and_win"))
  }
  
  # Team to win the opening faceoff
  if (grepl("team.*to.*win.*opening.*faceoff", market_lower)) {
    return(list(type = "team_win_faceoff"))
  }
  
  # Team to record first shot on goal
  if (grepl("team.*to.*record.*first.*shot", market_lower)) {
    return(list(type = "team_first_shot"))
  }
  
  # Team win with shutout
  if (grepl("team.*win.*with.*shutout", market_lower)) {
    return(list(type = "team_win_shutout"))
  }
  
  # Team regulation win with shutout
  if (grepl("team.*regulation.*win.*with.*shutout", market_lower)) {
    return(list(type = "team_regulation_win_shutout"))
  }
  
  # Team win from behind
  if (grepl("team.*win.*from.*behind", market_lower)) {
    return(list(type = "team_win_from_behind"))
  }
  
  # ==================== FALLBACK ====================
  debug_cat(sprintf("  WARNING: Could not classify NHL market: '%s', defaulting to 'unknown'\n", market_lower))
  return(list(type = "unknown", market = market_lower))
}

classify_market <- function(market_type, sport = NULL) {
  market_lower <- tolower(market_type)
  
  # DEBUG logging
  debug_cat(sprintf("Classifying market: %s (sport: %s)\n", market_type, sport %||% "unknown"))
  
  # ==================== SPORT-SPECIFIC MARKETS ====================
  if (!is.null(sport) && tolower(sport) %in% c("nhl", "hockey")) {
    return(classify_nhl_market(market_lower))
  }
  
  # ==================== PERIOD MARKETS (GENERAL) ====================
  
  # 1st Half markets
  if (grepl("1st half", market_lower)) {
    if (grepl("moneyline", market_lower)) {
      if (grepl("3-way", market_lower)) {
        return(list(type = "period_moneyline_3way", period = "1st_half"))
      }
      return(list(type = "period_moneyline", period = "1st_half"))
    }
    if (grepl("point spread|spread", market_lower)) {
      return(list(type = "period_spread", period = "1st_half"))
    }
    if (grepl("total points", market_lower)) {
      if (grepl("odd/even", market_lower)) {
        return(list(type = "period_total_oddeven", period = "1st_half"))
      }
      return(list(type = "period_total", period = "1st_half"))
    }
  }
  
  # 2nd Half markets
  if (grepl("2nd half", market_lower)) {
    if (grepl("excluding overtime", market_lower)) {
      if (grepl("moneyline", market_lower)) {
        return(list(type = "period_moneyline_3way", period = "2nd_half_excluding_ot"))
      }
    }
    if (grepl("moneyline", market_lower)) {
      if (grepl("3-way", market_lower)) {
        return(list(type = "period_moneyline_3way", period = "2nd_half"))
      }
      return(list(type = "period_moneyline", period = "2nd_half"))
    }
    if (grepl("point spread|spread", market_lower)) {
      return(list(type = "period_spread", period = "2nd_half"))
    }
    if (grepl("total points", market_lower)) {
      if (grepl("odd/even", market_lower)) {
        return(list(type = "period_total_oddeven", period = "2nd_half"))
      }
      return(list(type = "period_total", period = "2nd_half"))
    }
  }
  
  # ==================== QUARTER MARKETS ====================
  for (q in 1:4) {
    quarter_names <- c("1st", "2nd", "3rd", "4th")
    quarter_pattern <- paste0(quarter_names[q], " quarter")
    
    if (grepl(quarter_pattern, market_lower)) {
      # 4th quarter special handling
      if (q == 4) {
        if (grepl("excluding overtime", market_lower)) {
          if (grepl("moneyline", market_lower)) {
            if (grepl("3-way", market_lower)) {
              return(list(type = "period_moneyline_3way", period = "4th_quarter_excluding_ot"))
            }
            return(list(type = "period_moneyline", period = "4th_quarter_excluding_ot"))
          }
          if (grepl("point spread|spread", market_lower)) {
            return(list(type = "period_spread", period = "4th_quarter_excluding_ot"))
          }
          if (grepl("total points", market_lower)) {
            return(list(type = "period_total", period = "4th_quarter_excluding_ot"))
          }
        }
        if (grepl("including overtime", market_lower)) {
          if (grepl("moneyline", market_lower)) {
            if (grepl("3-way", market_lower)) {
              return(list(type = "period_moneyline_3way", period = "4th_quarter_including_ot"))
            }
            return(list(type = "period_moneyline", period = "4th_quarter_including_ot"))
          }
          if (grepl("point spread|spread", market_lower)) {
            return(list(type = "period_spread", period = "4th_quarter_including_ot"))
          }
          if (grepl("total points", market_lower)) {
            return(list(type = "period_total", period = "4th_quarter_including_ot"))
          }
        }
      }
      
      # Standard quarter markets
      if (grepl("moneyline", market_lower)) {
        if (grepl("3-way", market_lower)) {
          return(list(type = "period_moneyline_3way", period = paste0("quarter_", q)))
        }
        return(list(type = "period_moneyline", period = paste0("quarter_", q)))
      }
      if (grepl("point spread|spread", market_lower)) {
        return(list(type = "period_spread", period = paste0("quarter_", q)))
      }
      if (grepl("team to score last", market_lower)) {
        return(list(type = "period_team_score_last", period = paste0("quarter_", q)))
      }
      if (grepl("total points", market_lower)) {
        if (grepl("odd/even", market_lower)) {
          return(list(type = "period_total_oddeven", period = paste0("quarter_", q)))
        }
        return(list(type = "period_total", period = paste0("quarter_", q)))
      }
    }
  }
  
  # ==================== INNING MARKETS (BASEBALL) ====================
  # Single inning markets (1st, 2nd, 3rd, etc.)
  for (inning in 1:9) {
    inning_suffix <- ifelse(inning == 1, "st", ifelse(inning == 2, "nd", ifelse(inning == 3, "rd", "th")))
    inning_pattern <- paste0("^", inning, inning_suffix, " inning")
    
    if (grepl(inning_pattern, market_lower)) {
      if (grepl("moneyline", market_lower)) {
        if (grepl("3-way", market_lower)) {
          return(list(type = "inning_moneyline_3way", inning = inning))
        }
        return(list(type = "inning_moneyline", inning = inning))
      }
      if (grepl("run line", market_lower)) {
        return(list(type = "inning_run_line", inning = inning))
      }
      if (grepl("total runs", market_lower)) {
        if (grepl("odd/even", market_lower)) {
          return(list(type = "inning_total_runs_oddeven", inning = inning))
        }
        return(list(type = "inning_total_runs", inning = inning))
      }
      if (grepl("both teams to score", market_lower)) {
        return(list(type = "inning_both_teams_score", inning = inning))
      }
      if (grepl("total walks", market_lower)) {
        return(list(type = "inning_total_walks", inning = inning))
      }
      if (grepl("total pitches", market_lower)) {
        return(list(type = "inning_total_pitches", inning = inning))
      }
      if (grepl("total strikeouts", market_lower)) {
        return(list(type = "inning_total_strikeouts", inning = inning))
      }
      if (grepl("total doubles", market_lower)) {
        return(list(type = "inning_total_doubles", inning = inning))
      }
      if (grepl("total singles", market_lower)) {
        return(list(type = "inning_total_singles", inning = inning))
      }
      if (grepl("total triples", market_lower)) {
        return(list(type = "inning_total_triples", inning = inning))
      }
    }
  }
  
  # ==================== MULTI-INNING MARKETS (FIXED) ====================
  # Pattern: "1st 2 Innings", "1st 3 Innings", "1st 5 Innings", etc.
  multi_inning_match <- regexpr("1st\\s+([2-9])\\s+innings?", market_lower, ignore.case = TRUE)
  
  if (multi_inning_match != -1) {
    num_innings <- as.numeric(regmatches(market_lower, regexec("1st\\s+([2-9])\\s+innings?", market_lower, ignore.case = TRUE))[[1]][2])
    
    if (!is.na(num_innings)) {
      debug_cat(sprintf("  Multi-inning market detected: %d innings\n", num_innings))
      
      if (grepl("moneyline", market_lower)) {
        if (grepl("3-way", market_lower)) {
          return(list(type = "multi_inning_moneyline_3way", innings = num_innings))
        }
        return(list(type = "multi_inning_moneyline", innings = num_innings))
      }
      if (grepl("run line", market_lower)) {
        return(list(type = "multi_inning_run_line", innings = num_innings))
      }
      if (grepl("total runs", market_lower)) {
        return(list(type = "multi_inning_total_runs", innings = num_innings))
      }
      if (grepl("both teams to score", market_lower)) {
        return(list(type = "multi_inning_both_teams_score", innings = num_innings))
      }
      if (grepl("total hits", market_lower)) {
        return(list(type = "multi_inning_total_hits", innings = num_innings))
      }
      if (grepl("total rbis?", market_lower)) {
        return(list(type = "multi_inning_total_rbis", innings = num_innings))
      }
      if (grepl("total doubles", market_lower)) {
        return(list(type = "multi_inning_total_doubles", innings = num_innings))
      }
      if (grepl("total triples", market_lower)) {
        return(list(type = "multi_inning_total_triples", innings = num_innings))
      }
      if (grepl("total walks", market_lower)) {
        return(list(type = "multi_inning_total_walks", innings = num_innings))
      }
      if (grepl("plate appearances", market_lower)) {
        return(list(type = "multi_inning_total_pa", innings = num_innings))
      }
      if (grepl("total singles", market_lower)) {
        return(list(type = "multi_inning_total_singles", innings = num_innings))
      }
    }
  }
  
  # ==================== PERIOD MARKETS (HOCKEY) ====================
  for (p in 1:3) {
    period_names <- c("1st", "2nd", "3rd")
    period_pattern <- paste0(period_names[p], " period")
    
    if (grepl(period_pattern, market_lower)) {
      if (grepl("both teams to score", market_lower)) {
        return(list(type = "period_both_teams_score", period = p))
      }
      if (grepl("moneyline", market_lower)) {
        if (grepl("3-way", market_lower)) {
          return(list(type = "period_moneyline_3way", period = p))
        }
        return(list(type = "period_moneyline", period = p))
      }
      if (grepl("puck line", market_lower)) {
        return(list(type = "period_puck_line", period = p))
      }
      if (grepl("total goals", market_lower)) {
        if (grepl("odd/even", market_lower)) {
          return(list(type = "period_total_goals_oddeven", period = p))
        }
        return(list(type = "period_total_goals", period = p))
      }
      if (grepl("team to score first", market_lower)) {
        return(list(type = "period_team_score_first", period = p))
      }
      if (grepl("first 10 minutes total goals", market_lower)) {
        return(list(type = "period_first_minutes_goals", period = p, minutes = 10))
      }
      if (grepl("first 5 minutes total goals", market_lower)) {
        return(list(type = "period_first_minutes_goals", period = p, minutes = 5))
      }
    }
  }
  
  # ==================== PLAYER PERIOD MARKETS ====================
  # IMPORTANT: This must come BEFORE the generic PLAYER STAT MARKETS
  for (p in 1:4) {
    period_names <- c("1st", "2nd", "3rd", "4th")
    period_label <- period_names[p]
    
    # For quarters (basketball) - check for both "player X quarter" and "X quarter player"
    if (grepl("player", market_lower) && grepl(paste0(period_label, " quarter"), market_lower)) {
      if (grepl("points", market_lower)) {
        return(list(type = "player_period_stat", stat = "points", period = paste0("quarter_", p)))
      }
      if (grepl("rebounds", market_lower)) {
        return(list(type = "player_period_stat", stat = "rebounds", period = paste0("quarter_", p)))
      }
      if (grepl("assists", market_lower)) {
        return(list(type = "player_period_stat", stat = "assists", period = paste0("quarter_", p)))
      }
    }
    
    # For periods (hockey) - check for both "player X period" and "X period player"
    if (grepl("player", market_lower) && grepl(paste0(period_label, " period"), market_lower)) {
      if (grepl("goals", market_lower)) {
        return(list(type = "player_period_stat", stat = "goals", period = paste0("period_", p)))
      }
      if (grepl("assists", market_lower)) {
        return(list(type = "player_period_stat", stat = "assists", period = paste0("period_", p)))
      }
      if (grepl("points", market_lower)) {
        return(list(type = "player_period_stat", stat = "points", period = paste0("period_", p)))
      }
      if (grepl("shots on goal", market_lower)) {
        return(list(type = "player_period_stat", stat = "shots", period = paste0("period_", p)))
      }
    }
  }
  
  # ==================== PLAYER COMBO MARKETS ====================
  if (grepl("points.*rebounds.*assists|pra|p\\+r\\+a", market_lower)) {
    return(list(type = "player_combo", combo_type = "points_rebounds_assists"))
  }
  if (grepl("points.*rebounds|rebounds.*points|p\\+r", market_lower) && !grepl("assists", market_lower)) {
    return(list(type = "player_combo", combo_type = "points_rebounds"))
  }
  if (grepl("points.*assists|assists.*points|p\\+a", market_lower) && !grepl("rebounds", market_lower)) {
    return(list(type = "player_combo", combo_type = "points_assists"))
  }
  if (grepl("rebounds.*assists|assists.*rebounds|r\\+a", market_lower)) {
    return(list(type = "player_combo", combo_type = "rebounds_assists"))
  }
  if (grepl("blocks.*steals|steals.*blocks", market_lower)) {
    return(list(type = "player_combo", combo_type = "blocks_steals"))
  }
  
  # ==================== SPECIAL GAME MARKETS (MOVED HERE - BEFORE PLAYER STAT MARKETS) ====================
  if (grepl("largest comeback", market_lower)) {
    return(list(type = "largest_comeback"))
  }
  if (grepl("first run inning", market_lower)) {
    return(list(type = "first_run_inning"))
  }
  if (grepl("last run inning", market_lower)) {
    return(list(type = "last_run_inning"))
  }
  if (grepl("first scoring play runs", market_lower)) {
    return(list(type = "first_scoring_play_runs"))
  }
  if (grepl("goal in each period", market_lower)) {
    return(list(type = "goal_each_period"))
  }
  if (grepl("highest scoring period", market_lower)) {
    return(list(type = "highest_scoring_period", num_options = 4))
  }
  if (grepl("highest scoring quarter", market_lower)) {
    return(list(type = "highest_scoring_quarter", num_options = 5))
  }
  if (grepl("lowest scoring quarter", market_lower)) {
    return(list(type = "lowest_scoring_quarter", num_options = 5))
  }
  if (grepl("highest scoring half", market_lower)) {
    return(list(type = "highest_scoring_half", num_options = 3))
  }
  if (grepl("first field goal type", market_lower)) {
    if (grepl("4-way", market_lower)) {
      return(list(type = "first_field_goal_type", num_options = 4))
    }
    return(list(type = "first_field_goal_type", num_options = 2))
  }
  if (grepl("first home run type", market_lower)) {
    return(list(type = "first_home_run_type", num_options = 5))
  }
  if (grepl("first minute both teams to score", market_lower)) {
    return(list(type = "first_minute_both_score"))
  }
  # First X minutes total threes (MUST come before player stat patterns)
  if (grepl("first.*minutes.*total.*threes", market_lower)) {
    minutes <- as.numeric(gsub("[^0-9]", "", market_lower))
    return(list(type = "first_minutes_total_threes", minutes = minutes))
  }
  if (grepl("first 10 minutes total goals", market_lower)) {
    return(list(type = "first_minutes_total_goals", minutes = 10))
  }
  if (grepl("first 5 minutes total goals", market_lower)) {
    return(list(type = "first_minutes_total_goals", minutes = 5))
  }
  if (grepl("first 2 minutes total shots on goal", market_lower)) {
    return(list(type = "first_minutes_total_shots", minutes = 2))
  }
  
  # ==================== PLAYER SPECIAL MARKETS ====================
  # Check for "most" markets FIRST (more specific, before generic patterns)
  if (grepl("player to have most", market_lower)) {
    if (grepl("points", market_lower)) return(list(type = "player_special", special_type = "most_points"))
    if (grepl("rebounds", market_lower)) return(list(type = "player_special", special_type = "most_rebounds"))
    if (grepl("assists", market_lower)) return(list(type = "player_special", special_type = "most_assists"))
    if (grepl("threes", market_lower)) return(list(type = "player_special", special_type = "most_threes"))
    if (grepl("shots on goal", market_lower)) return(list(type = "player_special", special_type = "most_shots"))
  }
  
  if (grepl("double.*double", market_lower)) {
    return(list(type = "player_special", special_type = "double_double"))
  }
  if (grepl("triple.*double", market_lower)) {
    return(list(type = "player_special", special_type = "triple_double"))
  }
  
  # Generic threes - exclude "most" and "total" markets
  if (grepl("threes|3.*point|3pt|three.*point", market_lower) && !grepl("team", market_lower) && !grepl("most", market_lower) && !grepl("^total", market_lower)) {
    return(list(type = "player_stat", stat = "three_pointers_made"))
  }
  
  # Player record a win (MLB)
  if (grepl("player record a win", market_lower)) {
    return(list(type = "player_special", special_type = "win"))
  }
  
  # ==================== PLAYER FIRST/LAST MARKETS ====================
  if (grepl("player first", market_lower)) {
    if (grepl("basket", market_lower)) return(list(type = "first_scorer", scorer_type = "basket"))
    if (grepl("field goal", market_lower)) return(list(type = "first_scorer", scorer_type = "field_goal"))
    if (grepl("goal", market_lower)) return(list(type = "first_scorer", scorer_type = "goal"))
    if (grepl("assist", market_lower)) return(list(type = "first_scorer", scorer_type = "assist"))
    if (grepl("rebound", market_lower)) return(list(type = "first_scorer", scorer_type = "rebound"))
    if (grepl("three", market_lower)) return(list(type = "first_scorer", scorer_type = "three"))
    if (grepl("dunk", market_lower)) return(list(type = "first_scorer", scorer_type = "dunk"))
    if (grepl("home run", market_lower)) return(list(type = "first_scorer", scorer_type = "home_run"))
  }
  if (grepl("player last", market_lower)) {
    if (grepl("goal", market_lower)) return(list(type = "last_scorer", scorer_type = "goal"))
  }
  
  # ==================== PLAYER STAT MARKETS ====================
  # This comes AFTER SPECIAL GAME MARKETS so "First X Minutes Total Threes" is caught first
  player_stats <- c(
    "points", "rebounds", "assists", "steals", "blocks", "turnovers",
    "threes", "three_pointers_made", "field goals made", "field goals attempted",
    "free throws made", "free throws attempted", "minutes", "plus/minus",
    "personal fouls", "doubles", "triples", "home runs", "runs", "rbi",
    "hits", "stolen bases", "batting strikeouts", "batting walks",
    "pitching strikeouts", "earned runs allowed", "hits allowed", "walks allowed",
    "innings pitched", "outs recorded", "saves", "goals", "assists", "shots on goal",
    "faceoffs won", "blocked shots", "power play points", "time on ice",
    "penalty minutes", "fantasy score", "batting fantasy score", "pitching fantasy score",
    "singles", "doubles", "triples", "runs", "stolen_bases",
    "total bases", "total_bases", "hits", "home_runs", "rbi", "walks", "strikeouts",
    "pitching_strikeouts", "earned_runs", "innings_pitched", "outs"
  )
  
  for (stat in player_stats) {
    stat_pattern <- gsub(" ", ".*", stat)
    # IMPORTANT: Skip if it's a "Total" market (game total, not player stat)
    if (grepl("^total", market_lower) && !grepl("player", market_lower)) {
      next
    }
    if (grepl(paste0("player.*", stat_pattern), market_lower)) {
      return(list(type = "player_stat", stat = gsub(" ", "_", stat)))
    }
  }
  
  # ==================== TEAM MARKETS ====================
  if (grepl("team to score first", market_lower) && !grepl("period|inning", market_lower)) {
    if (grepl("3-way", market_lower)) {
      return(list(type = "team_score_first", bet_type = "3way"))
    }
    return(list(type = "team_score_first", bet_type = "2way"))
  }
  if (grepl("team to score last", market_lower)) {
    return(list(type = "team_score_last"))
  }
  if (grepl("team to win the most innings", market_lower)) {
    return(list(type = "team_most_innings"))
  }
  if (grepl("team to have most hits", market_lower)) {
    return(list(type = "team_most_hits", bet_type = "3way"))
  }
  if (grepl("team to have highest scoring", market_lower)) {
    if (grepl("inning", market_lower)) return(list(type = "team_highest_scoring_inning"))
    if (grepl("quarter", market_lower)) return(list(type = "team_highest_scoring_quarter"))
  }
  if (grepl("team to have highest three-point field goal percentage", market_lower)) {
    return(list(type = "team_highest_three_pct"))
  }
  if (grepl("team to record first shot on goal", market_lower)) {
    return(list(type = "team_first_shot"))
  }
  if (grepl("team to win the opening faceoff", market_lower)) {
    return(list(type = "team_win_faceoff"))
  }
  if (grepl("team win both halves", market_lower)) {
    return(list(type = "team_win_both_halves"))
  }
  if (grepl("team win every quarter", market_lower)) {
    return(list(type = "team_win_every_quarter"))
  }
  if (grepl("team win from behind", market_lower)) {
    return(list(type = "team_win_from_behind"))
  }
  if (grepl("team win with shutout", market_lower)) {
    return(list(type = "team_win_shutout"))
  }
  if (grepl("team regulation win with shutout", market_lower)) {
    return(list(type = "team_regulation_win_shutout"))
  }
  if (grepl("team score first and win", market_lower)) {
    return(list(type = "team_score_first_and_win"))
  }
  
  # ==================== TEAM TOTAL MARKETS ====================
  team_total_patterns <- list(
    "team total points" = "points",
    "team total runs" = "runs",
    "team total goals" = "goals",
    "team total hits" = "hits",
    "team total home runs" = "home_runs",
    "team total threes" = "threes",
    "team total assists" = "assists",
    "team total rebounds" = "rebounds",
    "team total steals" = "steals",
    "team total blocks" = "blocks",
    "team total turnovers" = "turnovers",
    "team total strikeouts" = "strikeouts",
    "team total pitching strikeouts" = "pitching_strikeouts",
    "team total walks" = "walks",
    "team total bases" = "total_bases",
    "team total extra base hits" = "extra_base_hits",
    "team total doubles" = "doubles",
    "team total triples" = "triples",
    "team total singles" = "singles",
    "team total power play goals" = "power_play_goals",
    "team total shorthanded goals" = "shorthanded_goals",
    "team total shots on goal" = "shots_on_goal",
    # Added missing team inning total patterns
    "team 1st inning total batters" = "batters",
    "team 1st inning total batting walks" = "batting_walks",
    "team 1st inning total pitches thrown" = "pitches_thrown",
    "team 1st inning total plate appearances" = "plate_appearances",
    "team 1st inning total rbis" = "rbis",
    "team 1st 5 innings total batting walks" = "batting_walks",
    "team 1st 5 innings total pitching strikeouts" = "pitching_strikeouts",
    "team total batting walks" = "batting_walks",
    "team total pitches thrown" = "pitches_thrown",
    "team total plate appearances" = "plate_appearances",
    "team total rbis" = "rbis",
    "team 1st inning total doubles" = "doubles",
    "team 1st inning total singles" = "singles",
    "team 1st inning total strikeouts thrown" = "strikeouts_thrown",
    "team 1st inning total triples" = "triples"
  )
  
  for (pattern in names(team_total_patterns)) {
    if (grepl(pattern, market_lower)) {
      if (grepl("odd/even", market_lower)) {
        return(list(type = "team_total_oddeven", stat = team_total_patterns[[pattern]]))
      }
      return(list(type = "team_total", stat = team_total_patterns[[pattern]]))
    }
  }
  
  # ==================== REGULATION MARKETS (MOVED BEFORE GAME MARKETS) ====================
  if (grepl("regulation", market_lower)) {
    if (grepl("moneyline", market_lower)) {
      if (grepl("3-way", market_lower)) {
        return(list(type = "regulation_moneyline", bet_type = "3way"))
      }
      return(list(type = "regulation_moneyline", bet_type = "2way"))
    }
    if (grepl("puck line", market_lower)) {
      return(list(type = "regulation_puck_line"))
    }
    if (grepl("total goals", market_lower)) {
      return(list(type = "regulation_total_goals"))
    }
    if (grepl("both teams to score", market_lower)) {
      return(list(type = "regulation_both_teams_score"))
    }
    if (grepl("team to score first", market_lower)) {
      if (grepl("3-way", market_lower)) {
        return(list(type = "regulation_team_score_first", bet_type = "3way"))
      }
      return(list(type = "regulation_team_score_first", bet_type = "2way"))
    }
    if (grepl("team to score last", market_lower)) {
      if (grepl("3-way", market_lower)) {
        return(list(type = "regulation_team_score_last", bet_type = "3way"))
      }
      return(list(type = "regulation_team_score_last", bet_type = "2way"))
    }
    if (grepl("shutout", market_lower)) {
      return(list(type = "regulation_shutout"))
    }
  }
  
  # ==================== GAME MARKETS ====================
  if (grepl("moneyline", market_lower)) {
    if (grepl("3-way", market_lower)) {
      return(list(type = "game_moneyline", bet_type = "3way"))
    }
    return(list(type = "game_moneyline", bet_type = "2way"))
  }
  if (grepl("point spread|spread", market_lower)) {
    return(list(type = "game_spread"))
  }
  if (grepl("run line", market_lower)) {
    return(list(type = "game_run_line"))
  }
  if (grepl("puck line", market_lower)) {
    return(list(type = "game_puck_line"))
  }
  
  # ========== SPECIFIC TOTAL MARKETS FIRST (MUST BE BEFORE GENERIC "total goals") ==========
  if (grepl("total power play goals", market_lower)) {
    return(list(type = "game_power_play_goals"))
  }
  if (grepl("total threes", market_lower)) {
    return(list(type = "game_total_threes"))
  }
  if (grepl("total shots on goal", market_lower)) {
    return(list(type = "game_total_shots"))
  }
  if (grepl("total grand slams", market_lower)) {
    return(list(type = "total_grand_slams"))
  }
  if (grepl("total home runs", market_lower)) {
    return(list(type = "total_home_runs"))
  }
  if (grepl("total hits", market_lower)) {
    return(list(type = "total_hits"))
  }
  # Added missing MLB total markets (These map to player_stat if no team prefix)
  if (grepl("total doubles", market_lower) && !grepl("team", market_lower)) {
    return(list(type = "player_stat", stat = "doubles"))
  }
  if (grepl("total plate appearances", market_lower) && !grepl("team", market_lower)) {
    return(list(type = "player_stat", stat = "plate_appearances"))
  }
  if (grepl("total rbis", market_lower) && !grepl("team", market_lower)) {
    return(list(type = "player_stat", stat = "rbis"))
  }
  if (grepl("total singles", market_lower) && !grepl("team", market_lower)) {
    return(list(type = "player_stat", stat = "singles"))
  }
  if (grepl("total strikeouts", market_lower) && !grepl("team", market_lower)) {
    return(list(type = "player_stat", stat = "strikeouts"))
  }
  if (grepl("total triples", market_lower) && !grepl("team", market_lower)) {
    return(list(type = "player_stat", stat = "triples"))
  }
  if (grepl("total walks", market_lower) && !grepl("team", market_lower)) {
    return(list(type = "player_stat", stat = "walks"))
  }
  if (grepl("total bases", market_lower) && !grepl("team|player", market_lower)) {
    return(list(type = "player_stat", stat = "total_bases"))
  }
  if (grepl("total extra base hits", market_lower) && !grepl("team", market_lower)) {
    return(list(type = "player_stat", stat = "extra_base_hits"))
  }
  
  # ========== GENERIC TOTAL MARKETS (LAST RESORT FOR TOTAL MARKETS) ==========
  if (grepl("total points", market_lower) || grepl("total runs", market_lower) || grepl("total goals", market_lower)) {
    if (grepl("odd/even", market_lower)) {
      return(list(type = "game_total_oddeven"))
    }
    return(list(type = "game_total"))
  }
  
  if (grepl("both teams to score", market_lower)) {
    return(list(type = "game_both_teams_score"))
  }
  if (grepl("will there be overtime", market_lower)) {
    return(list(type = "game_overtime"))
  }
  if (grepl("will there be extra innings", market_lower)) {
    return(list(type = "game_extra_innings"))
  }
  if (grepl("will home team bat in bottom of 9th", market_lower)) {
    return(list(type = "game_home_bat_bottom_9th"))
  }
  # Will there be a no-hitter
  if (grepl("will there be a no-hitter", market_lower)) {
    return(list(type = "game_special", special_type = "no_hitter"))
  }
  
  # ==================== TEAM PLAYER MARKETS ====================
  if (grepl("team player first", market_lower)) {
    if (grepl("basket", market_lower)) return(list(type = "team_player_first", stat = "basket"))
    if (grepl("field goal", market_lower)) return(list(type = "team_player_first", stat = "field_goal"))
    if (grepl("goal", market_lower)) return(list(type = "team_player_first", stat = "goal"))
    if (grepl("three", market_lower)) return(list(type = "team_player_first", stat = "three"))
  }
  
  # ==================== FALLBACK ====================
  debug_cat(sprintf("  WARNING: Could not classify market: '%s', defaulting to 'unknown'\n", market_type))
  return(list(type = "unknown", market = market_type))
}

resolve_combo_prop <- function(game_id, player_name, sport, season, combo_type, line_value = NULL) {
  debug_cat(sprintf("\nResolving combo prop: %s for %s\n", combo_type, player_name))
  
  tryCatch({
    if (sport %in% c("nba", "basketball")) {
      # Use your existing ESPN function
      stats <- get_espn_nba_player_stats(game_id, player_name)
      
      if (is.null(stats)) {
        return(list(success = FALSE, error = "Could not fetch player stats"))
      }
      
      # Calculate combo value
      actual_value <- 0
      components <- list()
      
      if (combo_type == "points_assists") {
        actual_value <- stats$points + stats$assists
        components <- list(points = stats$points, assists = stats$assists)
        debug_cat(sprintf("  Points: %s, Assists: %s, Total: %s\n",
                          stats$points, stats$assists, actual_value))
      } else if (combo_type == "rebounds_assists") {
        actual_value <- stats$rebounds + stats$assists
        components <- list(rebounds = stats$rebounds, assists = stats$assists)
        debug_cat(sprintf("  Rebounds: %s, Assists: %s, Total: %s\n",
                          stats$rebounds, stats$assists, actual_value))
      } else if (combo_type == "blocks_steals") {
        actual_value <- stats$blocks + stats$steals
        components <- list(blocks = stats$blocks, steals = stats$steals)
        debug_cat(sprintf("  Blocks: %s, Steals: %s, Total: %s\n",
                          stats$blocks, stats$steals, actual_value))
      } else if (combo_type == "points_rebounds_assists") {
        actual_value <- stats$points + stats$rebounds + stats$assists
        components <- list(points = stats$points, rebounds = stats$rebounds, assists = stats$assists)
        debug_cat(sprintf("  Points: %s, Rebounds: %s, Assists: %s, Total: %s\n",
                          stats$points, stats$rebounds, stats$assists, actual_value))
      } else if (combo_type == "points_rebounds") {
        actual_value <- stats$points + stats$rebounds
        components <- list(points = stats$points, rebounds = stats$rebounds)
        debug_cat(sprintf("  Points: %s, Rebounds: %s, Total: %s\n",
                          stats$points, stats$rebounds, actual_value))
      } else {
        return(list(success = FALSE, error = paste("Unknown combo type:", combo_type)))
      }
      
      result <- list(
        success = TRUE,
        actual_value = actual_value,
        components = components,
        combo_type = combo_type,
        player_stats = stats  # Include full stats for debugging
      )
      
      if (!is.null(line_value)) {
        result$line_value <- as.numeric(line_value)
        debug_cat(sprintf("  Line value: %s\n", result$line_value))
      }
      
      return(result)
      
    } else {
      return(list(success = FALSE, error = paste("Combo props not implemented for:", sport)))
    }
    
  }, error = function(e) {
    return(list(success = FALSE, error = paste("Error in resolve_combo_prop:", e$message)))
  })
}

# ==============================================
# TEST FUNCTION FOR MARKET CLASSIFICATION
# ==============================================

test_market_classification <- function() {
  cat("\n=== TESTING MARKET CLASSIFICATION ===\n")
  
  test_cases <- list(
    list(market = "Player Points + Assists", expected = "player_combo"),
    list(market = "player points + assists", expected = "player_combo"),
    list(market = "Player Points + Rebounds", expected = "player_combo"),
    list(market = "Player Rebounds + Assists", expected = "player_combo"),
    list(market = "Player Points + Rebounds + Assists", expected = "player_combo"),
    list(market = "Player Double Double", expected = "player_special"),
    list(market = "player double double", expected = "player_special"),
    list(market = "Player Triple Double", expected = "player_special"),
    list(market = "Player Threes", expected = "player_stat"),
    list(market = "player threes over", expected = "player_stat"),
    list(market = "Player To Have Most Points", expected = "player_special"),
    list(market = "Player To Have Most Rebounds", expected = "player_special"),
    list(market = "Player To Have Most Assists", expected = "player_special")
  )
  
  for (test in test_cases) {
    result <- classify_market(test$market)
    status <- ifelse(result$type == test$expected, "✓ PASS", "✗ FAIL")
    cat(sprintf("%-40s -> %-20s %s\n", test$market, result$type, status))
    
    if (result$type == "player_combo") {
      cat(sprintf("    Combo type: %s\n", result$combo_type))
    } else if (result$type == "player_special") {
      cat(sprintf("    Special type: %s\n", result$special_type))
    } else if (result$type == "player_stat") {
      cat(sprintf("    Stat: %s\n", result$stat))
    }
  }
  
  cat("=== TEST COMPLETE ===\n")
}

# ==============================================
# PERIOD TOTALS RESOLVER
# ==============================================

resolve_period_total <- function(game_id, sport, season, period, line_value = NULL) {
  debug_cat(sprintf("\nResolving %s total for %s\n", period, sport))
  
  tryCatch({
    if (sport %in% c("nba", "basketball")) {
      # Use ESPN API to get period scores
      url <- paste0("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event=", game_id)
      response <- GET(url, timeout = 10)
      
      if (status_code(response) == 200) {
        data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
        
        # Extract period scores from boxscore
        if (!is.null(data$boxscore) && !is.null(data$boxscore$periodScores)) {
          period_scores <- data$boxscore$periodScores
          
          total <- 0
          
          if (period == "1st_half" && nrow(period_scores) >= 2) {
            total <- sum(period_scores$homeScore[1:2], na.rm = TRUE) +
              sum(period_scores$awayScore[1:2], na.rm = TRUE)
          } else if (period == "2nd_half" && nrow(period_scores) >= 4) {
            total <- sum(period_scores$homeScore[3:4], na.rm = TRUE) +
              sum(period_scores$awayScore[3:4], na.rm = TRUE)
          } else if (period == "1st_quarter" && nrow(period_scores) >= 1) {
            total <- period_scores$homeScore[1] + period_scores$awayScore[1]
          }
          
          if (total > 0) {
            result <- list(success = TRUE, period = period, total = total)
            if (!is.null(line_value)) result$line_value <- as.numeric(line_value)
            return(result)
          }
        }
      }
      return(list(success = FALSE, error = "Could not extract period scores"))
      
    } else if (sport %in% c("nfl", "football")) {
      # For NFL, use nflreadr to get quarter scores
      pbp <- nflreadr::load_pbp(season)
      game_pbp <- pbp[pbp$game_id == game_id, ]
      
      if (nrow(game_pbp) == 0) {
        return(list(success = FALSE, error = "No play-by-play data for game"))
      }
      
      total <- 0
      
      if (period == "1st_half") {
        # Sum quarters 1-2
        q1_plays <- game_pbp[game_pbp$qtr == 1, ]
        q2_plays <- game_pbp[game_pbp$qtr == 2, ]
        total <- max(q2_plays$total_home_score, na.rm = TRUE) +
          max(q2_plays$total_away_score, na.rm = TRUE)
      } else if (period == "1st_quarter") {
        q1_plays <- game_pbp[game_pbp$qtr == 1, ]
        total <- max(q1_plays$total_home_score, na.rm = TRUE) +
          max(q1_plays$total_away_score, na.rm = TRUE)
      }
      
      if (total > 0) {
        result <- list(success = TRUE, period = period, total = total)
        if (!is.null(line_value)) result$line_value <- as.numeric(line_value)
        return(result)
      }
      
      return(list(success = FALSE, error = "Could not calculate period total"))
    }
    
    return(list(success = FALSE, error = paste("Sport not supported:", sport)))
    
  }, error = function(e) {
    return(list(success = FALSE, error = paste("Error:", e$message)))
  })
}

# ==============================================
# ESPN API FIRST/LAST SCORER FUNCTIONS FOR NBA
# ==============================================
fetch_first_scorer_espn_nba <- function(game_id, season) {
  debug_cat("  Using ESPN API for first scorer...\n")
  
  tryCatch({
    # First check game status
    game_status <- check_nba_game_status(game_id)
    
    if (!game_status$exists) {
      return(list(found = FALSE, error = paste("Game not found:", game_status$error)))
    }
    
    if (!game_status$completed) {
      return(list(found = FALSE, error = paste("Game not completed. Status:", game_status$status)))
    }
    
    # Get the summary data
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event=", game_id)
    debug_cat(sprintf("  Fetching from summary endpoint: %s\n", url))
    response <- GET(url, timeout = 15)
    
    if (status_code(response) == 200) {
      data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
      
      # Check if we have plays data
      if (!is.null(data$plays) && is.data.frame(data$plays)) {
        debug_cat(sprintf("  Found plays data frame with %d rows, %d columns\n",
                          nrow(data$plays), ncol(data$plays)))
        
        # Look for scoring plays
        if ("scoringPlay" %in% colnames(data$plays)) {
          scoring_plays <- data$plays[data$plays$scoringPlay == TRUE, ]
          debug_cat(sprintf("  Found %d scoring plays\n", nrow(scoring_plays)))
          
          if (nrow(scoring_plays) > 0) {
            # Get the first scoring play (lowest sequenceNumber)
            first_scoring <- scoring_plays[order(scoring_plays$sequenceNumber), ][1, ]
            debug_cat(sprintf("  First scoring play sequence: %s\n", first_scoring$sequenceNumber))
            debug_cat(sprintf("  Play text: %s\n", first_scoring$text))
            
            # Extract scorer from text
            scorer <- "Unknown"
            text <- first_scoring$text
            
            # Pattern: "Player Name makes/made X point shot"
            if (grepl("makes|made", text)) {
              # Extract everything before "makes" or "made"
              name_part <- strsplit(text, " makes | made ")[[1]][1]
              # Take the last word(s) as player name (handle Jr., III, etc.)
              scorer <- name_part
              debug_cat(sprintf("  Extracted scorer: %s\n", scorer))
            }
            
            # Determine which team scored based on score change
            team <- "Unknown"
            if (!is.null(first_scoring$awayScore) && !is.null(first_scoring$homeScore)) {
              # Get previous scores (look at the play before this one)
              play_index <- which(data$plays$id == first_scoring$id)
              if (play_index > 1) {
                prev_play <- data$plays[play_index - 1, ]
                prev_away <- prev_play$awayScore %||% 0
                prev_home <- prev_play$homeScore %||% 0
                
                if (first_scoring$awayScore > prev_away) {
                  # Away team scored
                  team <- game_status$away_team
                } else if (first_scoring$homeScore > prev_home) {
                  # Home team scored
                  team <- game_status$home_team
                }
                debug_cat(sprintf("  Team that scored: %s\n", team))
              }
            }
            
            # Get points
            points <- first_scoring$scoreValue %||% 0
            if (points == 0) {
              # Guess from text
              if (grepl("three", text, ignore.case = TRUE)) {
                points <- 3
              } else if (grepl("free.throw", text, ignore.case = TRUE)) {
                points <- 1
              } else {
                points <- 2  # Default assumption
              }
            }
            
            return(list(
              found = TRUE,
              scorer = scorer,
              team = team,
              points = points,
              quarter = first_scoring$period.number %||% 1,
              time = first_scoring$clock.displayValue %||% "Unknown",
              description = text,
              source = "ESPN Plays Data Frame"
            ))
          } else {
            debug_cat("  No scoring plays found\n")
          }
        } else {
          debug_cat("  No scoringPlay column found\n")
          
          # Alternative: Look for scoring by text
          scoring_text <- data$plays[grepl("makes|made|points", data$plays$text, ignore.case = TRUE), ]
          if (nrow(scoring_text) > 0) {
            first_scoring <- scoring_text[order(scoring_text$sequenceNumber), ][1, ]
            debug_cat("  Found first scoring play via text search\n")
            
            # Extract scorer
            scorer <- "Unknown"
            text <- first_scoring$text
            if (grepl("makes|made", text)) {
              name_part <- strsplit(text, " makes | made ")[[1]][1]
              scorer <- name_part
            }
            
            return(list(
              found = TRUE,
              scorer = scorer,
              team = "Unknown",
              points = 2,  # Assume 2 points
              quarter = first_scoring$period.number %||% 1,
              time = first_scoring$clock.displayValue %||% "Unknown",
              description = text,
              source = "ESPN Text Search"
            ))
          }
        }
      } else {
        debug_cat("  No plays data frame found\n")
      }
      
      return(list(found = FALSE, error = "No scoring plays found in ESPN data"))
      
    } else {
      return(list(found = FALSE, error = sprintf("ESPN API returned status %d", status_code(response))))
    }
    
  }, error = function(e) {
    return(list(found = FALSE, error = paste("ESPN API error:", e$message)))
  })
}

fetch_last_scorer_espn_nba <- function(game_id, season) {
  debug_cat("  Using ESPN API for last scorer...\n")
  
  tryCatch({
    # First check game status
    game_status <- check_nba_game_status(game_id)
    
    if (!game_status$exists) {
      return(list(found = FALSE, error = paste("Game not found:", game_status$error)))
    }
    
    if (!game_status$completed) {
      return(list(found = FALSE, error = paste("Game not completed. Status:", game_status$status)))
    }
    
    # Try summary endpoint first
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event=", game_id)
    debug_cat(sprintf("  Fetching from summary endpoint: %s\n", url))
    response <- GET(url, timeout = 15)
    
    if (status_code(response) == 200) {
      data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
      
      # Check if we have plays data
      if (!is.null(data$plays)) {
        debug_cat(sprintf("  Found plays data, type: %s\n", class(data$plays)))
        
        # Check structure of plays data
        if (is.list(data$plays) && length(data$plays) > 0) {
          # Try to extract plays from the list
          plays_list <- data$plays
          
          # Method 1: Check if first element is a data frame
          if (is.data.frame(plays_list[[1]])) {
            debug_cat("  Plays is a list of data frames\n")
            all_plays <- plays_list[[1]]
            
            # Look for scoring plays
            scoring_plays <- list()
            if ("scoringPlay" %in% colnames(all_plays)) {
              scoring_plays <- all_plays[all_plays$scoringPlay == TRUE, ]
            } else if ("type.text" %in% colnames(all_plays)) {
              scoring_plays <- all_plays[grepl("made|points|three|free throw",
                                               all_plays$type.text, ignore.case = TRUE), ]
            }
            
            if (nrow(scoring_plays) > 0) {
              last_play <- scoring_plays[order(scoring_plays$id, decreasing = TRUE), ][1, ]
              debug_cat(sprintf("  Found %d scoring plays, last play ID: %s\n",
                                nrow(scoring_plays), last_play$id))
              
              # Extract scorer information
              scorer <- "Unknown"
              scorer_id <- NULL
              
              if (!is.null(last_play$text)) {
                scorer <- last_play$text
                debug_cat(sprintf("  Scorer from text: %s\n", scorer))
              }
              
              if (!is.null(last_play$athletesInvolved)) {
                athletes <- last_play$athletesInvolved
                if (length(athletes) > 0) {
                  for (athlete in athletes) {
                    if (!is.null(athlete$displayName)) {
                      scorer <- athlete$displayName
                      scorer_id <- athlete$id
                      debug_cat(sprintf("  Scorer from athletes: %s (ID: %s)\n", scorer, scorer_id))
                      break
                    }
                  }
                }
              }
              
              # Get team info
              team <- "Unknown"
              if (!is.null(last_play$team) && !is.null(last_play$team$displayName)) {
                team <- last_play$team$displayName
              }
              
              # Get points
              points <- 0
              if (!is.null(last_play$scoreValue)) {
                points <- last_play$scoreValue
              } else {
                if (grepl("three", last_play$text, ignore.case = TRUE)) {
                  points <- 3
                } else if (grepl("free throw", last_play$text, ignore.case = TRUE)) {
                  points <- 1
                } else {
                  points <- 2
                }
              }
              
              return(list(
                found = TRUE,
                scorer = scorer,
                scorer_id = scorer_id,
                team = team,
                points = points,
                quarter = if (!is.null(last_play$period)) last_play$period$number else 1,
                time = if (!is.null(last_play$clock)) last_play$clock$displayValue else "Unknown",
                description = if (!is.null(last_play$text)) last_play$text else "Last score",
                source = "ESPN Summary API"
              ))
            }
          }
        }
      }
      
      # Fallback to old endpoint
      debug_cat("  Trying fallback endpoint...\n")
      url2 <- paste0("https://cdn.espn.com/core/nba/playbyplay?gameId=", game_id, "&xhr=1")
      response2 <- GET(url2, timeout = 15)
      
      if (status_code(response2) == 200) {
        data2 <- fromJSON(content(response2, "text", encoding = "UTF-8"))
        
        if (!is.null(data2$gamepackageJSON)) {
          game_data <- fromJSON(data2$gamepackageJSON)
          
          if (!is.null(game_data$plays)) {
            plays <- game_data$plays
            
            # Find last scoring play
            scoring_plays <- plays[!is.na(plays$scoringPlay) & plays$scoringPlay == TRUE, ]
            
            if (nrow(scoring_plays) > 0) {
              last_play <- scoring_plays[order(scoring_plays$id, decreasing = TRUE), ][1, ]
              
              scorer <- "Unknown"
              if (!is.null(last_play$athletesInvolved)) {
                athletes <- last_play$athletesInvolved
                if (length(athletes) > 0 && !is.null(athletes[[1]]$displayName)) {
                  scorer <- athletes[[1]]$displayName
                } else if (!is.null(last_play$text)) {
                  scorer <- last_play$text
                }
              } else if (!is.null(last_play$text)) {
                scorer <- last_play$text
              }
              
              return(list(
                found = TRUE,
                scorer = scorer,
                team = if (!is.null(last_play$teamId)) last_play$teamId else "Unknown",
                points = if (!is.null(last_play$scoreValue)) last_play$scoreValue else 0,
                quarter = if (!is.null(last_play$period)) last_play$period$number else 1,
                time = if (!is.null(last_play$clock)) last_play$clock$displayValue else "Unknown",
                source = "ESPN Play-by-Play API"
              ))
            }
          }
        }
      }
      
      return(list(found = FALSE, error = "No scoring plays found in ESPN data"))
      
    } else {
      return(list(found = FALSE, error = sprintf("ESPN API returned status %d", status_code(response))))
    }
    
  }, error = function(e) {
    return(list(found = FALSE, error = paste("ESPN API error:", e$message)))
  })
}

check_nba_game_status <- function(game_id) {
  debug_cat(sprintf("  Checking NBA game status for ID: %s\n", game_id))
  
  tryCatch({
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event=", game_id)
    debug_cat(sprintf("    URL: %s\n", url))
    response <- GET(url, timeout = 10)
    
    if (status_code(response) == 200) {
      data <- fromJSON(content(response, "text", encoding = "UTF-8"))
      
      if (!is.null(data$header) && !is.null(data$header$competitions) &&
          nrow(data$header$competitions) > 0) {
        
        competition <- data$header$competitions[1, ]
        
        # Get teams
        home_team <- "Unknown"
        away_team <- "Unknown"
        if (!is.null(competition$competitors)) {
          teams <- competition$competitors[[1]]
          for (i in 1:nrow(teams)) {
            if (teams$homeAway[i] == "home") home_team <- teams$team$displayName[i] %||% "Unknown"
            if (teams$homeAway[i] == "away") away_team <- teams$team$displayName[i] %||% "Unknown"
          }
        }
        
        # GET STATUS - SIMPLE DIRECT ACCESS (BASED ON DEBUG DATA)
        status <- "Unknown"
        status_type <- "Unknown"
        completed <- FALSE
        
        if (!is.null(competition$status) && is.data.frame(competition$status)) {
          # DIRECT ACCESS TO TYPE DATA FRAME (WE KNOW THIS WORKS)
          if ("type" %in% colnames(competition$status) &&
              is.data.frame(competition$status$type) &&
              nrow(competition$status$type) > 0) {
            
            # This is the CORRECT way based on our debug output
            type_info <- competition$status$type[1, ]
            status_type <- type_info$name %||% "Unknown"
            status <- type_info$description %||% "Unknown"
            completed <- as.logical(type_info$completed %||% FALSE)
          }
        }
        
        debug_cat(sprintf("    Game: %s @ %s\n", away_team, home_team))
        debug_cat(sprintf("    Status: %s (type: %s, completed: %s)\n", status, status_type, completed))
        
        return(list(
          exists = TRUE,
          home_team = home_team,
          away_team = away_team,
          status = status,
          status_type = status_type,
          completed = completed,
          error = NULL
        ))
      }
    }
    return(list(exists = FALSE, error = "Game not found"))
  }, error = function(e) {
    debug_cat(sprintf("    Error: %s\n", e$message))
    return(list(exists = FALSE, error = paste("Error:", e$message)))
  })
}

# ==============================================
# CHECK NHL GAME STATUS (DEDICATED FUNCTION)
# ==============================================

check_nhl_game_status <- function(game_id) {
  debug_cat(sprintf("  Checking NHL game status for ID: %s\n", game_id))
  
  tryCatch({
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary?event=", game_id)
    debug_cat(sprintf("    URL: %s\n", url))
    response <- GET(url, timeout = 10)
    
    if (status_code(response) == 200) {
      data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
      
      if (!is.null(data$header) && !is.null(data$header$competitions) &&
          nrow(data$header$competitions) > 0) {
        
        competition <- data$header$competitions[1, ]
        
        # Get teams
        home_team <- "Unknown"
        away_team <- "Unknown"
        if (!is.null(competition$competitors)) {
          teams <- competition$competitors[[1]]
          for (i in 1:nrow(teams)) {
            if (teams$homeAway[i] == "home") home_team <- teams$team$displayName[i] %||% "Unknown"
            if (teams$homeAway[i] == "away") away_team <- teams$team$displayName[i] %||% "Unknown"
          }
        }
        
        # Get game status
        status <- "Unknown"
        status_type <- "Unknown"
        completed <- FALSE
        
        if (!is.null(competition$status) && is.data.frame(competition$status)) {
          if ("type" %in% colnames(competition$status) &&
              is.data.frame(competition$status$type) &&
              nrow(competition$status$type) > 0) {
            
            type_info <- competition$status$type[1, ]
            status_type <- type_info$name %||% "Unknown"
            status <- type_info$description %||% "Unknown"
            completed <- as.logical(type_info$completed %||% FALSE)
          }
        }
        
        # Get scores if available
        home_score <- 0
        away_score <- 0
        if (!is.null(competition$competitors)) {
          teams <- competition$competitors[[1]]
          for (i in 1:nrow(teams)) {
            if (teams$homeAway[i] == "home") {
              home_score <- as.numeric(teams$score[i] %||% 0)
            } else if (teams$homeAway[i] == "away") {
              away_score <- as.numeric(teams$score[i] %||% 0)
            }
          }
        }
        
        debug_cat(sprintf("    Game: %s @ %s\n", away_team, home_team))
        debug_cat(sprintf("    Score: %d - %d\n", away_score, home_score))
        debug_cat(sprintf("    Status: %s (type: %s, completed: %s)\n", status, status_type, completed))
        
        return(list(
          exists = TRUE,
          home_team = home_team,
          away_team = away_team,
          home_score = home_score,
          away_score = away_score,
          status = status,
          status_type = status_type,
          completed = completed,
          error = NULL
        ))
      }
    }
    return(list(exists = FALSE, error = "Game not found"))
  }, error = function(e) {
    debug_cat(sprintf("    Error: %s\n", e$message))
    return(list(exists = FALSE, error = paste("Error:", e$message)))
  })
}

# ==============================================
# DIRECT GAME ID LOOKUP BY VERIFICATION
# ==============================================

find_nba_game_id_by_verification <- function(game_date, away_team, home_team) {
  debug_cat(sprintf("  Trying direct game ID verification for %s @ %s on %s...\n",
                    away_team, home_team, game_date))
  
  # Helper function to clean team names for comparison
  clean_team_name_for_match <- function(name) {
    if (is.null(name) || is.na(name) || name == "Unknown") return("")
    # Convert to lowercase and remove non-alphanumeric
    name <- tolower(name)
    name <- gsub("[^a-z0-9]", "", name)
    name
  }
  
  # Clean expected team names
  expected_home_clean <- clean_team_name_for_match(home_team)
  expected_away_clean <- clean_team_name_for_match(away_team)
  
  debug_cat(sprintf("  Expected teams cleaned: home='%s', away='%s'\n",
                    expected_home_clean, expected_away_clean))
  
  # Common NBA game ID patterns
  base_date_num <- as.numeric(format(as.Date(game_date), "%Y%m%d"))
  
  # Generate possible game IDs
  possible_ids <- c()
  
  # Pattern 1: 40 + date + 2-digit sequence (most common)
  for (i in 1:20) {
    seq_num <- sprintf("%02d", i)  # 01, 02, 03, etc.
    possible_ids <- c(possible_ids, paste0("40", base_date_num, seq_num))
  }
  
  # Pattern 2: Check adjacent days (±3 days)
  for (day_offset in -3:3) {
    if (day_offset == 0) next  # Skip current day (already included)
    offset_date_num <- base_date_num + day_offset
    for (i in 1:10) {
      seq_num <- sprintf("%02d", i)
      possible_ids <- c(possible_ids, paste0("40", offset_date_num, seq_num))
    }
  }
  
  # Pattern 3: Known working IDs for specific matchups
  if (expected_home_clean == "bostonceltics" && expected_away_clean == "indianapacers") {
    possible_ids <- c("401810476", possible_ids)  # Known working ID
    debug_cat("  Added known game ID 401810476 for Pacers @ Celtics\n")
  }
  
  # Remove duplicates
  possible_ids <- unique(possible_ids)
  debug_cat(sprintf("  Testing %d possible game IDs...\n", length(possible_ids)))
  
  # Test each possible ID
  found_ids <- list()
  
  for (i in 1:length(possible_ids)) {
    game_id <- possible_ids[i]
    
    # Skip if we already tested too many
    if (i > 50) {
      debug_cat("  Stopping after testing 50 IDs to avoid rate limiting\n")
      break
    }
    
    debug_cat(sprintf("    Testing ID %s... ", game_id))
    
    # Check if this game exists
    game_status <- check_nba_game_status(game_id)
    
    if (game_status$exists) {
      # Clean actual team names
      actual_home_clean <- clean_team_name_for_match(game_status$home_team)
      actual_away_clean <- clean_team_name_for_match(game_status$away_team)
      
      debug_cat(sprintf("exists (%s @ %s)\n", game_status$away_team, game_status$home_team))
      
      # Check if teams match (allow swapped order)
      teams_match <- FALSE
      match_type <- "none"
      
      if (actual_home_clean == expected_home_clean && actual_away_clean == expected_away_clean) {
        teams_match <- TRUE
        match_type <- "exact"
      } else if (actual_home_clean == expected_away_clean && actual_away_clean == expected_home_clean) {
        teams_match <- TRUE
        match_type <- "swapped"
      } else if (nchar(expected_home_clean) > 3 && nchar(expected_away_clean) > 3) {
        # Check partial matches
        home_partial_match <- grepl(expected_home_clean, actual_home_clean) ||
          grepl(actual_home_clean, expected_home_clean)
        away_partial_match <- grepl(expected_away_clean, actual_away_clean) ||
          grepl(actual_away_clean, expected_away_clean)
        
        if (home_partial_match && away_partial_match) {
          teams_match <- TRUE
          match_type <- "partial"
        }
      }
      
      if (teams_match) {
        debug_cat(sprintf("    ✓ MATCH (%s)! %s @ %s\n", match_type,
                          game_status$away_team, game_status$home_team))
        
        # Store match info
        found_ids[[length(found_ids) + 1]] <- list(
          game_id = game_id,
          away_team = game_status$away_team,
          home_team = game_status$home_team,
          match_type = match_type,
          completed = game_status$completed
        )
      }
    } else {
      debug_cat("not found\n")
    }
    
    # Small delay to avoid rate limiting
    if (i %% 5 == 0) {
      Sys.sleep(0.1)
    }
  }
  
  # Choose the best match
  if (length(found_ids) > 0) {
    debug_cat(sprintf("  Found %d potential matches\n", length(found_ids)))
    
    # Prefer exact matches
    exact_matches <- found_ids[sapply(found_ids, function(x) x$match_type == "exact")]
    if (length(exact_matches) > 0) {
      best_match <- exact_matches[[1]]
      debug_cat(sprintf("  Using exact match: %s\n", best_match$game_id))
      return(best_match$game_id)
    }
    
    # Then swapped matches
    swapped_matches <- found_ids[sapply(found_ids, function(x) x$match_type == "swapped")]
    if (length(swapped_matches) > 0) {
      best_match <- swapped_matches[[1]]
      debug_cat(sprintf("  Using swapped match: %s\n", best_match$game_id))
      return(best_match$game_id)
    }
    
    # Then partial matches
    partial_matches <- found_ids[sapply(found_ids, function(x) x$match_type == "partial")]
    if (length(partial_matches) > 0) {
      best_match <- partial_matches[[1]]
      debug_cat(sprintf("  Using partial match: %s\n", best_match$game_id))
      return(best_match$game_id)
    }
    
    # Fallback: first match
    best_match <- found_ids[[1]]
    debug_cat(sprintf("  Using first match: %s\n", best_match$game_id))
    return(best_match$game_id)
  }
  
  debug_cat("  No matching game ID found via verification\n")
  return(NA)
}

# ==============================================
# SIMPLIFIED VERSION (if the above is too complex)
# ==============================================

find_nba_game_id_simple_verification <- function(game_date, away_team, home_team) {
  debug_cat(sprintf("  Simple verification for %s @ %s on %s\n",
                    away_team, home_team, game_date))
  
  # Known working game IDs (add more as you find them)
  known_games <- list(
    # Format: "home_team @ away_team" = "game_id"
    "boston celtics @ indiana pacers" = "401810476",
    "indiana pacers @ boston celtics" = "401810476",
    # Add more known games here
    "detroit pistons @ boston celtics" = "401836803",
    "washington wizards @ indiana pacers" = "401836797"
  )
  
  # Create lookup key
  lookup_key <- tolower(paste(home_team, "@", away_team))
  reverse_key <- tolower(paste(away_team, "@", home_team))
  
  debug_cat(sprintf("  Looking up: '%s'\n", lookup_key))
  debug_cat(sprintf("  Reverse lookup: '%s'\n", reverse_key))
  
  # Check known games
  if (lookup_key %in% names(known_games)) {
    game_id <- known_games[[lookup_key]]
    debug_cat(sprintf("  ✓ Found in known games: %s\n", game_id))
    return(game_id)
  }
  
  if (reverse_key %in% names(known_games)) {
    game_id <- known_games[[reverse_key]]
    debug_cat(sprintf("  ✓ Found reversed in known games: %s\n", game_id))
    return(game_id)
  }
  
  # If not in known games, try a few common patterns
  debug_cat("  Not in known games, trying common patterns...\n")
  
  base_date_num <- as.numeric(format(as.Date(game_date), "%Y%m%d"))
  
  # Try a few common patterns
  test_ids <- c(
    paste0("40", base_date_num, "01"),
    paste0("40", base_date_num, "02"),
    paste0("40", base_date_num, "03"),
    paste0("40", base_date_num, "04"),
    paste0("40", base_date_num, "05")
  )
  
  # Add known working ID for Pacers @ Celtics
  if (grepl("celtics", tolower(home_team)) && grepl("pacers", tolower(away_team))) {
    test_ids <- c("401810476", test_ids)
  }
  
  for (game_id in test_ids) {
    debug_cat(sprintf("    Testing %s... ", game_id))
    game_status <- check_nba_game_status(game_id)
    
    if (game_status$exists) {
      debug_cat(sprintf("exists\n"))
      
      # Simple check: do team names contain expected words?
      home_match <- grepl(tolower(home_team), tolower(game_status$home_team)) ||
        grepl(tolower(game_status$home_team), tolower(home_team))
      away_match <- grepl(tolower(away_team), tolower(game_status$away_team)) ||
        grepl(tolower(game_status$away_team), tolower(away_team))
      
      if (home_match && away_match) {
        debug_cat(sprintf("    ✓ Teams match! %s @ %s\n",
                          game_status$away_team, game_status$home_team))
        return(game_id)
      }
    } else {
      debug_cat("not found\n")
    }
  }
  
  debug_cat("  No matching game ID found\n")
  return(NA)
}

detect_correct_season <- function(sport, game_date, current_year = NULL) {
  debug_cat(sprintf("DEBUG detect_correct_season: sport=%s, game_date=%s\n", sport, game_date))
  
  if (is.null(current_year)) {
    current_year <- lubridate::year(Sys.Date())
  }
  
  game_date_obj <- as.Date(game_date)
  game_year <- lubridate::year(game_date_obj)
  game_month <- lubridate::month(game_date_obj)
  
  sport <- tolower(sport)
  
  if (sport %in% c("nfl", "football")) {
    # NFL season runs Sep-Feb (spans calendar years)
    # Example: Game on Dec 7, 2025 -> Season 2025
    # Example: Game on Jan 10, 2026 -> Also Season 2025 (playoffs)
    # Example: Game on Sep 5, 2025 -> Season 2025
    
    if (game_month >= 9) {
      # Sep-Dec: Season is same year
      season <- game_year
    } else if (game_month <= 2) {
      # Jan-Feb: Season was previous year (playoffs)
      season <- game_year - 1
    } else {
      # Mar-Aug: Offseason, use previous season
      season <- game_year - 1
    }
    
    debug_cat(sprintf("  NFL: Game %s-%s -> Season %s (current year: %s)\n",
                      game_year, game_month, season, current_year))
    
  } else if (sport %in% c("nba", "basketball")) {
    # NBA season runs Oct-Jun
    # Example: Game on Dec 14, 2025 -> Season 2025
    # Example: Game on Jan 14, 2026 -> Also Season 2025
    
    if (game_month >= 10) {
      # Oct-Dec: Season starts this year
      season <- game_year
    } else {
      # Jan-Sep: Season started previous year
      season <- game_year - 1
    }
    
    debug_cat(sprintf("  NBA: Game %s-%s -> Season %s (current year: %s)\n",
                      game_year, game_month, season, current_year))
    
  } else {
    # Default: Use game year
    season <- game_year
    debug_cat(sprintf("  Other sport: Using game year %s (current year: %s)\n", season, current_year))
  }
  
  return(season)
}

# ==============================================
# CHECK DATA AVAILABILITY WITH FALLBACK
# ==============================================

check_and_fix_season_availability <- function(sport, season, game_date) {
  debug_cat(sprintf("DEBUG check_and_fix_season_availability: sport=%s, season=%s, game_date=%s\n",
                    sport, season, game_date))
  
  if (sport %in% c("nfl", "football")) {
    # Check if nflverse has data for this season
    available_seasons <- tryCatch({
      nflreadr::available_seasons()
    }, error = function(e) {
      debug_cat(sprintf("  Could not check available seasons: %s\n", e$message))
      NULL
    })
    
    if (!is.null(available_seasons)) {
      debug_cat(sprintf("  Available NFL seasons: %s\n", paste(available_seasons, collapse=", ")))
      
      if (!season %in% available_seasons) {
        debug_cat(sprintf("  ⚠️ Season %s not available yet\n", season))
        
        # Find the latest available season
        latest_available <- max(available_seasons)
        debug_cat(sprintf("  Latest available: %s\n", latest_available))
        
        # If game is recent (within last 2 seasons), use latest available
        game_date_obj <- as.Date(game_date)
        current_date <- Sys.Date()
        days_since_game <- as.numeric(difftime(current_date, game_date_obj, units = "days"))
        
        debug_cat(sprintf("  Game was %s days ago\n", days_since_game))
        
        if (days_since_game < 730) {  # 2 years
          debug_cat(sprintf("  Game is recent, using latest available season %s\n", latest_available))
          return(latest_available)
        } else {
          debug_cat(sprintf("  Game is old, trying season %s anyway\n", season))
        }
      } else {
        debug_cat(sprintf("  ✓ Season %s is available\n", season))
      }
    }
  }
  
  # Return original season if available or if not NFL
  return(season)
}

# ==============================================
# TEAM NAME MAPPING DATABASE
# ==============================================

team_mappings <- list(
  nfl = list(
    "chiefs" = "KC", "kansas city" = "KC", "kc" = "KC",
    "ravens" = "BAL", "baltimore" = "BAL", "bal" = "BAL",
    "49ers" = "SF", "san francisco" = "SF", "sf" = "SF",
    "lions" = "DET", "detroit" = "DET", "det" = "DET",
    "bills" = "BUF", "buffalo" = "BUF", "buf" = "BUF",
    "packers" = "GB", "green bay" = "GB", "gb" = "GB",
    "cowboys" = "DAL", "dallas" = "DAL", "dal" = "DAL",
    "eagles" = "PHI", "philadelphia" = "PHI",
    "dolphins" = "MIA", "miami" = "MIA", "mia" = "MIA",
    "texans" = "HOU", "houston" = "HOU", "hou" = "HOU",
    "bengals" = "CIN", "cincinnati" = "CIN", "cin" = "CIN",
    "steelers" = "PIT", "pittsburgh" = "PIT", "pit" = "PIT",
    "rams" = "LA", "los angeles rams" = "LA", "lar" = "LA",
    "browns" = "CLE", "cleveland" = "CLE", "cle" = "CLE",
    "seahawks" = "SEA", "seattle" = "SEA", "sea" = "SEA",
    "saints" = "NO", "new orleans" = "NO", "no" = "NO",
    "jets" = "NYJ", "new york jets" = "NYJ", "nyj" = "NYJ",
    "falcons" = "ATL", "atlanta" = "ATL", "atl" = "ATL",
    "raiders" = "LV", "las vegas" = "LV", "oakland" = "LV", "lv" = "LV",
    "cardinals" = "ARI", "arizona" = "ARI", "ari" = "ARI",
    "commanders" = "WAS", "washington" = "WAS", "was" = "WAS",
    "broncos" = "DEN", "denver" = "DEN", "den" = "DEN",
    "vikings" = "MIN", "minnesota" = "MIN", "min" = "MIN",
    "buccaneers" = "TB", "tampa bay" = "TB", "tb" = "TB",
    "colts" = "IND", "indianapolis" = "IND", "ind" = "IND",
    "bears" = "CHI", "chicago" = "CHI", "chi" = "CHI",
    "panthers" = "CAR", "carolina" = "CAR", "car" = "CAR",
    "titans" = "TEN", "tennessee" = "TEN", "ten" = "TEN",
    "giants" = "NYG", "new york giants" = "NYG", "nyg" = "NYG",
    "patriots" = "NE", "new england" = "NE", "ne" = "NE",
    "chargers" = "LAC", "los angeles chargers" = "LAC", "lac" = "LAC",
    "jaguars" = "JAX", "jacksonville" = "JAX", "jax" = "JAX"
  ),
  nba = list(
    # CRITICAL FIX: ESPN uses "LA Clippers" not "Los Angeles Clippers"
    "clippers" = "LAC", "la clippers" = "LAC", "laclippers" = "LAC", "losangelesclippers" = "LAC",
    "lakers" = "LAL", "los angeles lakers" = "LAL", "la lakers" = "LAL", "lalakers" = "LAL",
    "warriors" = "GS", "golden state" = "GS", "gsw" = "GS",
    "celtics" = "BOS", "boston" = "BOS", "bos" = "BOS",
    "suns" = "PHX", "phoenix" = "PHX", "phx" = "PHX",
    "bucks" = "MIL", "milwaukee" = "MIL", "mil" = "MIL",
    "nuggets" = "DEN", "denver" = "DEN", "den" = "DEN",
    "76ers" = "PHI", "philadelphia" = "PHI", "sixers" = "PHI", "phi" = "PHI",
    "heat" = "MIA", "miami" = "MIA", "mia" = "MIA",
    "knicks" = "NY", "new york" = "NY", "nyk" = "NY",
    "mavericks" = "DAL", "dallas" = "DAL", "mavs" = "DAL",
    "cavaliers" = "CLE", "cleveland" = "CLE", "cavs" = "CLE",
    "kings" = "SAC", "sacramento" = "SAC", "sac" = "SAC",
    "pacers" = "IND", "indiana" = "IND", "ind" = "IND",
    "timberwolves" = "MIN", "minnesota" = "MIN", "wolves" = "MIN",
    "thunder" = "OKC", "oklahoma city" = "OKC", "okc" = "OKC",
    "magic" = "ORL", "orlando" = "ORL", "orl" = "ORL",
    "pelicans" = "NO", "new orleans" = "NO", "nop" = "NO",
    "bulls" = "CHI", "chicago" = "CHI", "chi" = "CHI",
    "hawks" = "ATL", "atlanta" = "ATL", "atl" = "ATL",
    "nets" = "BKN", "brooklyn" = "BKN", "bkn" = "BKN",
    "rockets" = "HOU", "houston" = "HOU", "hou" = "HOU",
    "jazz" = "UTAH", "utah" = "UTAH", "uta" = "UTAH",
    "spurs" = "SA", "san antonio" = "SA", "sas" = "SA",
    "grizzlies" = "MEM", "memphis" = "MEM", "mem" = "MEM",
    "trail blazers" = "POR", "portland" = "POR", "blazers" = "POR",
    "wizards" = "WAS", "washington" = "WAS", "was" = "WAS",
    "hornets" = "CHA", "charlotte" = "CHA", "cha" = "CHA",
    "pistons" = "DET", "detroit" = "DET", "det" = "DET",
    "raptors" = "TOR", "toronto" = "TOR", "tor" = "TOR",
    
    # Additional mappings based on test results
    "atlantahawks" = "ATL", "bostonceltics" = "BOS", "chicagobulls" = "CHI",
    "clevelandcavaliers" = "CLE", "indianapacers" = "IND", "losangeleslakers" = "LAL",
    "torontoraptors" = "TOR"
  )
)


# ==============================================
# ESPN DATE FORMAT CONVERSION
# ==============================================

convert_espn_date_format <- function(date_input) {
  # Handle numeric Unix timestamps (like 19723)
  if (is.numeric(date_input)) {
    debug_cat(sprintf("  Converting numeric date: %s -> ", date_input))
    result <- as.Date(date_input, origin = "1970-01-01")
    debug_cat(sprintf("%s\n", result))
    return(result)
  }
  
  # Handle character dates
  if (is.character(date_input)) {
    # Clean the input
    date_clean <- gsub("['\"]", "", date_input)
    
    # Try various formats
    formats <- c("%Y-%m-%d", "%Y%m%d", "%m/%d/%Y", "%d.%m.%Y", "%Y.%m.%d")
    
    for (fmt in formats) {
      parsed_date <- try(as.Date(date_clean, format = fmt), silent = TRUE)
      if (!is.na(parsed_date) && !inherits(parsed_date, "try-error")) {
        debug_cat(sprintf("  Converted '%s' with format '%s' -> %s\n", date_input, fmt, parsed_date))
        return(parsed_date)
      }
    }
    
    # Try lubridate parsing
    parsed_date <- try(lubridate::parse_date_time(date_clean, orders = c("ymd", "mdy", "dmy", "ymd_HMS", "ymd_HM", "ymd_H")), silent = TRUE)
    if (!inherits(parsed_date, "try-error") && !is.na(parsed_date)) {
      debug_cat(sprintf("  Converted '%s' with lubridate -> %s\n", date_input, as.Date(parsed_date)))
      return(as.Date(parsed_date))
    }
  }
  
  # If it's already a Date object
  if (inherits(date_input, "Date")) {
    return(date_input)
  }
  
  # Default to today with warning
  debug_cat(sprintf("  WARNING: Could not convert date '%s' (type: %s), defaulting to today\n",
                    date_input, class(date_input)[1]))
  return(Sys.Date())
}

# ==============================================
# ESPN API GAME ID SEARCH WITH DATE FLEXIBILITY
# ==============================================
find_nba_game_id_single_date <- function(game_date, away_team_search, home_team_search) {
  tryCatch({
    espn_date <- format(as.Date(game_date), "%Y%m%d")
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates=", espn_date)
    debug_cat(sprintf("    Calling ESPN API: %s\n", url))
    response <- GET(url, timeout = 10)
    
    if (status_code(response) != 200) {
      debug_cat(sprintf("    API returned status: %d\n", status_code(response)))
      return(NA)
    }
    
    # Use flatten=TRUE to simplify the structure
    json_text <- content(response, "text", encoding = "UTF-8")
    data <- fromJSON(json_text, flatten = TRUE)
    
    if (is.null(data$events) || nrow(data$events) == 0) {
      debug_cat(sprintf("    No events found for date %s\n", game_date))
      return(NA)
    }
    
    games <- data$events
    debug_cat(sprintf("    Found %d games on %s\n", nrow(games), game_date))
    
    # Simple team name cleaner
    clean_team_name <- function(name) {
      if (is.null(name) || is.na(name)) return("")
      # Convert to lowercase and remove non-alphanumeric
      name <- tolower(name)
      name <- gsub("[^a-z0-9]", "", name)
      name
    }
    
    # Clean search terms
    home_search_clean <- clean_team_name(home_team_search)
    away_search_clean <- clean_team_name(away_team_search)
    
    debug_cat(sprintf("    Looking for: %s @ %s\n", away_team_search, home_team_search))
    debug_cat(sprintf("    Cleaned search: '%s' @ '%s'\n", away_search_clean, home_search_clean))
    
    # Search through games - FIXED EXTRACTION LOGIC
    for (i in 1:nrow(games)) {
      game <- games[i, ]
      game_id <- game$id
      
      # FIXED: Correct way to extract teams from flattened ESPN response
      home_team <- "Unknown"
      away_team <- "Unknown"
      
      # The competitions column contains a list
      if (!is.null(game$competitions)) {
        competitions_list <- game$competitions
        
        if (is.list(competitions_list) && length(competitions_list) > 0) {
          # Get first competition
          comp <- competitions_list[[1]]
          
          # FIXED: Handle flattened structure
          if (is.data.frame(comp)) {
            if (!is.null(comp$competitors)) {
              # competitors is a list column with data frames
              competitors_list <- comp$competitors
              
              if (is.list(competitors_list) && length(competitors_list) > 0) {
                # Get the competitors data frame
                competitors_df <- competitors_list[[1]]
                
                if (is.data.frame(competitors_df)) {
                  # Now we can extract home and away teams
                  for (j in 1:nrow(competitors_df)) {
                    competitor <- competitors_df[j, ]
                    
                    # FIXED: Check for flattened column names
                    if ("homeAway" %in% colnames(competitor)) {
                      if (!is.na(competitor$homeAway) && competitor$homeAway == "home") {
                        if ("team.displayName" %in% colnames(competitor)) {
                          home_team <- competitor$team.displayName
                        } else if ("team.name" %in% colnames(competitor)) {
                          home_team <- competitor$team.name
                        }
                      } else if (!is.na(competitor$homeAway) && competitor$homeAway == "away") {
                        if ("team.displayName" %in% colnames(competitor)) {
                          away_team <- competitor$team.displayName
                        } else if ("team.name" %in% colnames(competitor)) {
                          away_team <- competitor$team.name
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
      
      # Clean found teams
      home_clean <- clean_team_name(home_team)
      away_clean <- clean_team_name(away_team)
      
      debug_cat(sprintf("    Game %d: %s @ %s (ID: %s)\n", i, away_team, home_team, game_id))
      debug_cat(sprintf("    Cleaned: '%s' @ '%s'\n", away_clean, home_clean))
      
      # Check for match
      home_match <- grepl(home_search_clean, home_clean, ignore.case = TRUE) ||
        grepl(home_clean, home_search_clean, ignore.case = TRUE)
      away_match <- grepl(away_search_clean, away_clean, ignore.case = TRUE) ||
        grepl(away_clean, away_search_clean, ignore.case = TRUE)
      
      # Also check swapped teams
      swapped_home_match <- grepl(home_search_clean, away_clean, ignore.case = TRUE)
      swapped_away_match <- grepl(away_search_clean, home_clean, ignore.case = TRUE)
      
      if ((home_match && away_match) || (swapped_home_match && swapped_away_match)) {
        debug_cat(sprintf("    ✓ MATCH FOUND! Using ID: %s\n", game_id))
        return(game_id)
      } else {
        debug_cat("    No match\n")
      }
    }
    
    debug_cat("    No matching games found\n")
    
    # CRITICAL: Show what we found for debugging
    debug_cat("    All games on this date:\n")
    for (i in 1:nrow(games)) {
      game <- games[i, ]
      game_id <- game$id
      
      # Use the same extraction logic as above
      home_team <- "Unknown"
      away_team <- "Unknown"
      
      if (!is.null(game$competitions)) {
        competitions_list <- game$competitions
        
        if (is.list(competitions_list) && length(competitions_list) > 0) {
          comp <- competitions_list[[1]]
          
          if (is.data.frame(comp) && !is.null(comp$competitors)) {
            competitors_list <- comp$competitors
            
            if (is.list(competitions_list) && length(competitions_list) > 0) {
              competitors_df <- competitions_list[[1]]
              
              if (is.data.frame(competitors_df)) {
                for (j in 1:nrow(competitors_df)) {
                  competitor <- competitors_df[j, ]
                  
                  if (!is.null(competitor$homeAway)) {
                    if (!is.na(competitor$homeAway) && competitor$homeAway == "home") {
                      if (!is.null(competitor$team.displayName)) {
                        home_team <- competitor$team.displayName
                      } else if (!is.null(competitor$team.name)) {
                        home_team <- competitor$team.name
                      }
                    } else if (!is.na(competitor$homeAway) && competitor$homeAway == "away") {
                      if (!is.null(competitor$team.displayName)) {
                        away_team <- competitor$team.displayName
                      } else if (!is.null(competitor$team.name)) {
                        away_team <- competitor$team.name
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
      debug_cat(sprintf("      %s @ %s (ID: %s)\n", away_team, home_team, game_id))
    }
    
    return(NA)
    
  }, error = function(e) {
    debug_cat(sprintf("    Error checking date %s: %s\n", game_date, e$message))
    debug_cat(sprintf("    Stack trace: %s\n", paste(capture.output(traceback()), collapse = "\n")))
    return(NA)
  })
}

# ==============================================
# TIMEZONE-AWARE NBA GAME FINDING
# ==============================================

# Simple timezone-aware version
find_nba_game_id_timezone_simple <- function(game_date, away_team_search, home_team_search) {
  debug_cat(sprintf("🕐 Timezone-aware search for %s @ %s on %s\n",
                    away_team_search, home_team_search, game_date))
  
  base_date <- as.Date(game_date)
  
  # Try exact date first
  debug_cat("  1. Trying exact date first...\n")
  game_id <- find_nba_game_id_single_date(base_date, away_team_search, home_team_search)
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on exact date! Game ID: %s\n", game_id))
    return(list(
      game_id = game_id,
      actual_date = as.character(base_date),
      searched_date = as.character(base_date),
      days_off = 0,
      note = "Found on exact date"
    ))
  }
  
  # If not found, try previous day (for timezone issues)
  prev_date <- base_date - 1
  debug_cat(sprintf("  2. Trying previous day %s (for timezone issues)...\n", prev_date))
  game_id <- find_nba_game_id_single_date(prev_date, away_team_search, home_team_search)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on previous day! Game ID: %s\n", game_id))
    debug_cat(sprintf("    ⚠️ Timezone issue detected: Game appears 1 day earlier in ESPN\n"))
    debug_cat(sprintf("    This happens when games start after 8 PM Eastern\n"))
    
    return(list(
      game_id = game_id,
      actual_date = as.character(prev_date),
      searched_date = as.character(base_date),
      days_off = -1,
      note = "Found on previous day (timezone issue)"
    ))
  }
  
  # If still not found, try next day
  next_date <- base_date + 1
  debug_cat(sprintf("  3. Trying next day %s...\n", next_date))
  game_id <- find_nba_game_id_single_date(next_date, away_team_search, home_team_search)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on next day! Game ID: %s\n", game_id))
    debug_cat(sprintf("    ⚠️ Game appears 1 day later in ESPN\n"))
    
    return(list(
      game_id = game_id,
      actual_date = as.character(next_date),
      searched_date = as.character(base_date),
      days_off = 1,
      note = "Found on next day"
    ))
  }
  # If still not found, use wider search
  debug_cat("  4. Not found with timezone fix, using wider search (±3 days)...\n")
  return(find_nba_game_id_flexible(game_date, away_team_search, home_team_search, max_days_range = 14))
  
}

# ==============================================
# FLEXIBLE NBA GAME ID SEARCH WITH WIDE DATE RANGE
# ==============================================

# ==============================================
# FLEXIBLE NBA GAME ID SEARCH WITH WIDE DATE RANGE
# ==============================================

find_nba_game_id_flexible <- function(game_date, away_team_search, home_team_search, max_days_range = 14) {
  debug_cat(sprintf("🔄 Flexible NBA game search for %s @ %s around %s (±%d days)\n",
                    away_team_search, home_team_search, game_date, max_days_range))
  
  base_date <- as.Date(game_date)
  
  # Try dates in order: exact, -1, +1, -2, +2, etc.
  # But prioritize closer dates first
  date_offsets <- c(0)
  for (i in 1:max_days_range) {
    date_offsets <- c(date_offsets, -i, i)
  }
  
  # Clean search terms
  clean_team_name <- function(name) {
    if (is.null(name) || is.na(name)) return("")
    name <- tolower(name)
    name <- gsub("[^a-z0-9]", "", name)
    name
  }
  
  home_search_clean <- clean_team_name(home_team_search)
  away_search_clean <- clean_team_name(away_team_search)
  
  debug_cat(sprintf("  Cleaned search terms: home='%s', away='%s'\n",
                    home_search_clean, away_search_clean))
  
  # First, try to find ANY game with these teams in the range
  all_matches <- list()
  
  for (offset in date_offsets) {
    current_date <- base_date + offset
    debug_cat(sprintf("  Checking date %s (offset: %+d)... ", current_date, offset))
    
    game_id <- find_nba_game_id_single_date(current_date, away_team_search, home_team_search)
    
    if (!is.na(game_id)) {
      debug_cat(sprintf("✓ FOUND! Game ID: %s\n", game_id))
      
      # Get game details to verify
      game_status <- check_nba_game_status(game_id)
      
      if (game_status$exists) {
        # Store match with priority based on date closeness
        priority_score <- 100 - abs(offset) * 5  # Higher score for closer dates
        
        all_matches[[length(all_matches) + 1]] <- list(
          game_id = game_id,
          actual_date = as.character(current_date),
          offset = offset,
          priority = priority_score,
          home_team = game_status$home_team,
          away_team = game_status$away_team,
          completed = game_status$completed
        )
      }
    } else {
      debug_cat("no game\n")
    }
    
    # Small delay to avoid rate limiting
    if (abs(offset) %% 3 == 0) {
      Sys.sleep(0.05)
    }
  }
  
  # ========== ADDED: Team-name-only search fallback ==========
  if (length(all_matches) == 0) {
    debug_cat("  Trying team-name-only search (no date match)...\n")
    
    # Get the correct season
    season_year <- detect_correct_season("nba", game_date)
    debug_cat(sprintf("  Using NBA season: %s\n", season_year))
    
    # Load NBA schedule
    schedule <- tryCatch({
      hoopR::load_nba_schedule(season = season_year)
    }, error = function(e) {
      debug_cat(sprintf("  Error loading NBA schedule: %s\n", e$message))
      NULL
    })
    
    if (!is.null(schedule) && nrow(schedule) > 0) {
      schedule$game_date <- as.Date(schedule$game_date)
      
      # Find all games with these teams (order doesn't matter)
      # Clean the schedule names the same way we cleaned the search terms (remove spaces/punctuation)
      schedule_home_clean <- gsub("[^a-z0-9]", "", tolower(schedule$home_name))
      schedule_away_clean <- gsub("[^a-z0-9]", "", tolower(schedule$away_name))
      
      team_matches <- schedule[
        (grepl(home_search_clean, schedule_home_clean) &
           grepl(away_search_clean, schedule_away_clean)) |
          (grepl(home_search_clean, schedule_away_clean) &
             grepl(away_search_clean, schedule_home_clean)),
      ]
      
      if (nrow(team_matches) > 0) {
        debug_cat(sprintf("  Found %d potential team matches\n", nrow(team_matches)))
        
        # Calculate date difference and find closest
        team_matches$date_diff <- abs(as.Date(team_matches$game_date) - base_date)
        best_match <- team_matches[order(team_matches$date_diff), ][1, ]
        
        debug_cat(sprintf("  ✓ Found team match! Game ID: %s on %s (diff: %d days)\n", 
                          best_match$game_id, best_match$game_date, best_match$date_diff))
        
        return(list(
          game_id = best_match$game_id,
          actual_date = as.character(best_match$game_date),
          searched_date = as.character(base_date),
          days_off = as.numeric(difftime(as.Date(best_match$game_date), base_date, units = "days")),
          note = "Found via team-name-only search",
          all_matches_found = 1
        ))
      } else {
        debug_cat("  No team matches found in schedule\n")
      }
    }
  }
  # ========== END ADDED SECTION ==========
  
  if (length(all_matches) > 0) {
    # Sort matches by priority (closest date first)
    all_matches <- all_matches[order(sapply(all_matches, function(x) x$priority), decreasing = TRUE)]
    
    best_match <- all_matches[[1]]
    debug_cat(sprintf("  Selected best match: Game ID %s on %s (offset: %+d days)\n",
                      best_match$game_id, best_match$actual_date, best_match$offset))
    debug_cat(sprintf("    Teams: %s @ %s\n", best_match$away_team, best_match$home_team))
    
    return(list(
      game_id = best_match$game_id,
      actual_date = best_match$actual_date,
      searched_date = as.character(base_date),
      days_off = best_match$offset,
      note = sprintf("Found with %+d day offset", best_match$offset),
      all_matches_found = length(all_matches)
    ))
  }
  
  debug_cat(sprintf("  No game found in %d-day range\n", max_days_range * 2))
  
  # Try direct verification as last resort
  debug_cat("  Trying direct verification as last resort...\n")
  verified_id <- find_nba_game_id_by_verification(base_date, away_team_search, home_team_search)
  
  if (!is.na(verified_id)) {
    debug_cat(sprintf("  ✓ Found via direct verification: %s\n", verified_id))
    return(list(
      game_id = verified_id,
      actual_date = as.character(base_date),
      searched_date = as.character(base_date),
      days_off = 0,
      note = "Found via direct verification",
      all_matches_found = 1
    ))
  }
  
  # Show all games in the date range for debugging
  debug_cat("  Showing all games found in date range for debugging:\n")
  start_date <- base_date - max_days_range
  end_date <- base_date + max_days_range
  
  for (offset in -max_days_range:max_days_range) {
    current_date <- base_date + offset
    games <- get_all_nba_games_on_date(current_date)
    
    if (length(games) > 0) {
      debug_cat(sprintf("    Games on %s:\n", current_date))
      for (game in games) {
        debug_cat(sprintf("      %s (ID: %s) Score: %s\n",
                          game$matchup, game$game_id, game$score))
      }
    }
  }
  
  return(NULL)
}

get_all_nba_games_on_date <- function(game_date) {
  tryCatch({
    espn_date <- format(as.Date(game_date), "%Y%m%d")
    url <- paste0("http://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates=", espn_date)
    
    response <- GET(url, timeout = 10)
    
    if (status_code(response) != 200) {
      return(list())
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    if (is.null(data$events) || nrow(data$events) == 0) {
      return(list())
    }
    
    games_list <- list()
    
    for (i in 1:nrow(data$events)) {
      game <- data$events[i, ]
      
      if (!is.null(game$competitions) && length(game$competitions) > 0) {
        comp <- game$competitions[[1]]
        
        if (!is.null(comp$competitors) && length(comp$competitors) > 0) {
          competitors <- comp$competitors[[1]]
          
          # FIX: Check if competitors is a data frame
          if (!is.data.frame(competitors)) {
            next  # Skip this game if competitors is not a data frame
          }
          
          home_team <- "Unknown"
          away_team <- "Unknown"
          home_score <- NA
          away_score <- NA
          
          # Find teams and scores - FIXED: Use nrow() only on data frames
          for (j in 1:nrow(competitors)) {
            competitor <- competitors[j, ]
            
            # FIXED: Check for flattened column names
            if ("homeAway" %in% colnames(competitor)) {
              if (!is.na(competitor$homeAway) && competitor$homeAway == "home") {
                if ("team.displayName" %in% colnames(competitor)) {
                  home_team <- competitor$team.displayName
                }
                if ("score" %in% colnames(competitor)) {
                  home_score <- competitor$score
                }
              } else if (!is.na(competitor$homeAway) && competitor$homeAway == "away") {
                if ("team.displayName" %in% colnames(competitor)) {
                  away_team <- competitor$team.displayName
                }
                if ("score" %in% colnames(competitor)) {
                  away_score <- competitor$score
                }
              }
            }
          }
          
          games_list[[length(games_list) + 1]] <- list(
            game_id = game$id,
            matchup = paste(away_team, "@", home_team),
            score = paste(away_score, "-", home_score),
            status = if ("status.type.description" %in% colnames(game)) game$status.type.description else "Unknown"
          )
        }
      }
    }
    
    return(games_list)
    
  }, error = function(e) {
    debug_cat(sprintf("Error getting games on date %s: %s\n", game_date, e$message))
    debug_cat(sprintf("Stack trace: %s\n", paste(capture.output(traceback()), collapse = "\n")))
    return(list())
  })
}

# ==============================================
# NHL DATA FUNCTIONS FOR ESPN
# ==============================================

# Get NHL games from ESPN API for a specific date
get_espn_nhl_games <- function(game_date) {
  tryCatch({
    espn_date <- format(as.Date(game_date), "%Y%m%d")
    url <- paste0("http://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates=", espn_date)
    
    debug_cat(sprintf("  Calling ESPN NHL API for games on %s\n", espn_date))
    debug_cat(sprintf("  URL: %s\n", url))
    response <- GET(url, timeout = 10)
    
    if (status_code(response) != 200) {
      debug_cat(sprintf("  ESPN NHL API returned status: %d\n", status_code(response)))
      return(NULL)
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    if (is.null(data$events)) {
      debug_cat("  No NHL events found in ESPN response (events is NULL)\n")
      return(NULL)
    }
    
    # Handle both data frame and list cases
    if (is.data.frame(data$events)) {
      num_games <- nrow(data$events)
    } else if (is.list(data$events)) {
      num_games <- length(data$events)
    } else {
      debug_cat(sprintf("  Unexpected events type: %s\n", class(data$events)))
      return(NULL)
    }
    
    if (num_games == 0) {
      debug_cat("  No NHL events found in ESPN response (0 games)\n")
      return(NULL)
    }
    
    debug_cat(sprintf("  ESPN NHL API returned %d games\n", num_games))
    return(data$events)
    
  }, error = function(e) {
    debug_cat(sprintf("  ESPN NHL API error: %s\n", e$message))
    return(NULL)
  })
}

# Get NHL game period data from ESPN
# Get NHL game period data from ESPN - FIXED VERSION
get_espn_nhl_period_data <- function(game_id) {
  debug_cat(sprintf("  Calling ESPN NHL summary API for game: %s\n", game_id))
  
  tryCatch({
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary?event=", game_id)
    debug_cat(sprintf("  URL: %s\n", url))
    response <- GET(url, timeout = 10)
    
    if (status_code(response) != 200) {
      debug_cat(sprintf("  ESPN NHL API returned status: %d\n", status_code(response)))
      return(list(success = FALSE, error = paste("API returned status", status_code(response))))
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    # ========== SAFETY CHECK - Handle empty data ==========
    if (is.null(data) || length(data) == 0) {
      debug_cat("  ⚠️ ESPN returned empty data for NHL game\n")
      return(list(
        success = TRUE,
        periods = list(),
        game_data = list(
          home_team = "Unknown",
          away_team = "Unknown",
          home_score = 0,
          away_score = 0,
          status = "Unknown"
        )
      ))
    }
    
    # DEBUG: Log the structure to help debug
    debug_cat(sprintf("  DEBUG: Response structure - names: %s\n", paste(names(data), collapse=", ")))
    # ========== END SAFETY CHECK ==========
    
    result <- list(
      success = TRUE,
      periods = list(),
      game_data = list()
    )
    
    # Get period scores from boxscore
    if (!is.null(data$boxscore) && !is.null(data$boxscore$teams)) {
      teams_data <- data$boxscore$teams
      
      # Extract period-by-period scoring
      if (is.data.frame(teams_data)) {
        for (i in 1:nrow(teams_data)) {
          team_info <- teams_data[i, ]
          if (!is.null(team_info$homeAway)) {
            if (team_info$homeAway == "home") {
              result$game_data$home_team <- team_info$team$displayName %||% team_info$team.displayName
              result$game_data$home_score <- as.numeric(team_info$score %||% 0)
            } else {
              result$game_data$away_team <- team_info$team$displayName %||% team_info$team.displayName
              result$game_data$away_score <- as.numeric(team_info$score %||% 0)
            }
          }
        }
      }
    }
    
    # Try to get period-level data from linescores
    if (!is.null(data$header) && !is.null(data$header$competitions)) {
      comps <- data$header$competitions
      if (is.data.frame(comps) && nrow(comps) > 0) {
        # Add safety check for competitors
        if (!is.null(comps$competitors) && length(comps$competitors) > 0) {
          competitors <- comps$competitors[[1]]
          
          # Add safety check - ensure competitors is a data frame
          if (is.data.frame(competitors)) {
            for (i in 1:nrow(competitors)) {
              comp <- competitors[i, ]
              side <- comp$homeAway %||% "unknown"
              
              # Extract linescores (period scores) - add safety check
              if (!is.null(comp$linescores) && is.list(comp$linescores) && length(comp$linescores) > 0) {
                linescores <- comp$linescores[[1]]
                if (is.data.frame(linescores)) {
                  for (p in 1:nrow(linescores)) {
                    if (length(result$periods) < p || is.null(result$periods[[p]])) {
                      result$periods[[p]] <- list(period = p, home_score = 0, away_score = 0)
                    }
                    if (side == "home") {
                      result$periods[[p]]$home_score <- as.numeric(linescores$value[p] %||% 0)
                    } else if (side == "away") {
                      result$periods[[p]]$away_score <- as.numeric(linescores$value[p] %||% 0)
                    }
                  }
                }
              }
              
              # Get team names and final scores - add safety check
              if (side == "home") {
                result$game_data$home_team <- comp$team$displayName %||% comp$team.displayName %||% result$game_data$home_team
                result$game_data$home_score <- as.numeric(comp$score %||% result$game_data$home_score)
              } else if (side == "away") {
                result$game_data$away_team <- comp$team$displayName %||% comp$team.displayName %||% result$game_data$away_team
                result$game_data$away_score <- as.numeric(comp$score %||% result$game_data$away_score)
              }
            }
          }
        }
        
        # Check game status for OT - add safety check
        if (!is.null(comps$status) && is.list(comps$status) && length(comps$status) > 0) {
          status_info <- comps$status[[1]]
          if (!is.null(status_info$type$description)) {
            result$game_data$status <- status_info$type$description
            result$game_data$went_to_ot <- grepl("OT|Overtime|SO|Shootout", result$game_data$status, ignore.case = TRUE)
          }
        }
      }
    }
    
    # Get plays for period-specific events - add safety check
    if (!is.null(data$plays)) {
      result$plays <- data$plays
    }
    
    debug_cat(sprintf("  NHL data extracted: %s %d - %d %s\n",
                      result$game_data$away_team %||% "Unknown", result$game_data$away_score %||% 0,
                      result$game_data$home_score %||% 0, result$game_data$home_team %||% "Unknown"))
    debug_cat(sprintf("  Periods found: %d\n", length(result$periods)))
    
    return(result)
    
  }, error = function(e) {
    debug_cat(sprintf("  ERROR in get_espn_nhl_period_data: %s\n", e$message))
    return(list(success = FALSE, error = paste("Error:", e$message)))
  })
}

# FIXED: Get NHL player stats from ESPN (with goalie support)
get_espn_nhl_player_stats <- function(game_id, player_name) {
  tryCatch({
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary?event=", game_id)
    debug_cat(sprintf("  Calling ESPN NHL API for player stats: %s\n", url))
    
    response <- GET(url, timeout = 10)
    if (status_code(response) != 200) {
      debug_cat(sprintf("  ESPN NHL API returned status: %d\n", status_code(response)))
      return(NULL)
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    # Clean name for matching
    clean_name_for_matching <- function(name) {
      name <- tolower(trimws(name))
      name <- gsub("[[:punct:]]", "", name)
      name <- gsub("\\s+", " ", name)
      name
    }
    
    target_name <- clean_name_for_matching(player_name)
    debug_cat(sprintf("  Searching for NHL player: %s\n", target_name))
    
    # Check for boxscore players
    if (!is.null(data$boxscore) && !is.null(data$boxscore$players)) {
      players_df <- data$boxscore$players
      debug_cat(sprintf("  Found %d team(s) in NHL boxscore\n", nrow(players_df)))
      
      for (team_idx in 1:nrow(players_df)) {
        team_name <- players_df$team$displayName[team_idx]
        team_stats <- players_df$statistics[[team_idx]]
        
        if (is.data.frame(team_stats)) {
          # Loop through all stat categories (skaters, goalies, etc.)
          for (row_idx in 1:nrow(team_stats)) {
            # Get athletes for this row
            if (!is.null(team_stats$athletes[row_idx]) && is.list(team_stats$athletes[row_idx])) {
              athletes_data <- team_stats$athletes[[row_idx]]
              labels <- team_stats$labels[[row_idx]]
              
              if (is.data.frame(athletes_data) && nrow(athletes_data) > 0) {
                for (athlete_idx in 1:nrow(athletes_data)) {
                  athlete_row <- athletes_data[athlete_idx, ]
                  
                  # Extract athlete name
                  athlete_name <- NULL
                  if (!is.null(athlete_row$athlete$displayName)) {
                    athlete_name <- athlete_row$athlete$displayName
                  } else if (!is.null(athlete_row$athlete.displayName)) {
                    athlete_name <- athlete_row$athlete.displayName
                  }
                  
                  if (!is.null(athlete_name)) {
                    cleaned_athlete <- clean_name_for_matching(athlete_name)
                    
                    if (grepl(target_name, cleaned_athlete, fixed = TRUE) ||
                        grepl(cleaned_athlete, target_name, fixed = TRUE)) {
                      debug_cat(sprintf("  Found NHL player match: %s (Team: %s)\n", athlete_name, team_name))
                      
                      # Extract stats vector
                      stats_vector <- NULL
                      if (!is.null(athlete_row$stats) && is.list(athlete_row$stats) && length(athlete_row$stats) > 0) {
                        stats_vector <- athlete_row$stats[[1]]
                      }
                      
                      # Initialize result
                      result <- list(
                        player = athlete_name,
                        team = team_name,
                        goals = 0,
                        assists = 0,
                        points = 0,
                        plus_minus = 0,
                        penalty_minutes = 0,
                        shots = 0,
                        hits = 0,
                        blocked_shots = 0,
                        faceoffs_won = 0,
                        time_on_ice = "0:00",
                        saves = 0,
                        goals_against = 0,
                        save_percentage = 0,
                        shots_against = 0
                      )
                      
                      # Parse stats based on labels
                      if (!is.null(stats_vector) && !is.null(labels) && length(stats_vector) == length(labels)) {
                        for (j in seq_along(labels)) {
                          label <- tolower(labels[j])
                          val <- stats_vector[j]
                          
                          # Skater stats
                          if (label == "g") {
                            result$goals <- as.numeric(val) %||% 0
                          } else if (label == "a") {
                            result$assists <- as.numeric(val) %||% 0
                          } else if (label == "pts") {
                            result$points <- as.numeric(val) %||% 0
                          } else if (label == "+/-") {
                            result$plus_minus <- as.numeric(val) %||% 0
                          } else if (label == "pim") {
                            result$penalty_minutes <- as.numeric(val) %||% 0
                          } else if (label == "sog") {
                            result$shots <- as.numeric(val) %||% 0
                          } else if (label == "hits") {
                            result$hits <- as.numeric(val) %||% 0
                          } else if (label == "bs") {
                            result$blocked_shots <- as.numeric(val) %||% 0
                          } else if (label == "fow") {
                            result$faceoffs_won <- as.numeric(val) %||% 0
                          } else if (label == "toi") {
                            result$time_on_ice <- val %||% "0:00"
                          }
                          
                          # Goalie stats
                          else if (label == "sv") {
                            result$saves <- as.numeric(val) %||% 0
                          } else if (label == "ga") {
                            result$goals_against <- as.numeric(val) %||% 0
                          } else if (label == "sv%") {
                            result$save_percentage <- as.numeric(val) %||% 0
                          } else if (label == "sa") {
                            result$shots_against <- as.numeric(val) %||% 0
                          }
                        }
                      }
                      
                      # Calculate points if not provided
                      if (result$points == 0 && (result$goals > 0 || result$assists > 0)) {
                        result$points <- result$goals + result$assists
                      }
                      
                      debug_cat(sprintf("  NHL Stats: G=%d, A=%d, P=%d, SOG=%d, SV=%d, GA=%d\n",
                                        result$goals, result$assists, result$points, 
                                        result$shots, result$saves, result$goals_against))
                      
                      return(result)
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    
    debug_cat("  NHL player not found in boxscore\n")
    return(NULL)
    
  }, error = function(e) {
    debug_cat(sprintf("  Error getting NHL player stats: %s\n", e$message))
    return(NULL)
  })
}

# Get NHL player period stats from play-by-play
get_espn_nhl_player_period_stats <- function(game_id, player_name, period = NULL) {
  tryCatch({
    # First get all player stats
    player_data <- get_espn_nhl_player_stats(game_id, player_name)
    
    if (is.null(player_data)) {
      return(list(success = FALSE, error = "Player not found"))
    }
    
    # ESPN doesn't provide period-level stats directly
    # We need to parse plays data to get period stats
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary?event=", game_id)
    response <- GET(url, timeout = 10)
    
    if (status_code(response) != 200) {
      return(list(success = FALSE, error = "Failed to fetch play-by-play data"))
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    result <- list(
      success = TRUE,
      player = player_name,
      period = period,
      goals = 0,
      assists = 0,
      points = 0,
      shots = 0,
      saves = 0
    )
    
    if (!is.null(data$plays) && is.data.frame(data$plays)) {
      plays <- data$plays
      
      # Filter plays by player name
      player_plays <- plays[grepl(player_name, plays$text, ignore.case = TRUE), ]
      
      # Filter by period if specified
      if (!is.null(period) && !is.null(plays$period)) {
        period_num <- as.numeric(gsub("[^0-9]", "", period))
        if (!is.null(plays$period$number)) {
          player_plays <- player_plays[player_plays$period$number == period_num, ]
        }
      }
      
      # Count events
      if (nrow(player_plays) > 0) {
        result$goals <- sum(grepl("goal", player_plays$text, ignore.case = TRUE))
        result$assists <- sum(grepl("assist", player_plays$text, ignore.case = TRUE))
        result$shots <- sum(grepl("shot", player_plays$text, ignore.case = TRUE))
        result$saves <- sum(grepl("save", player_plays$text, ignore.case = TRUE))
        result$points <- result$goals + result$assists
      }
    }
    
    return(result)
    
  }, error = function(e) {
    return(list(success = FALSE, error = paste("Error:", e$message)))
  })
}

# Resolve NHL period markets - FIXED with safety checks
resolve_nhl_period_market <- function(game_id, market_info, market_lower, line_value = NULL) {
  debug_cat(sprintf("  Resolving NHL period market: %s\n", market_info$type))
  
  tryCatch({
    period_data <- get_espn_nhl_period_data(game_id)
    
    if (!period_data$success) {
      return(list(success = FALSE, error = period_data$error))
    }
    
    result <- list(success = TRUE)
    
    # Handle different period market types
    if (market_info$type == "period_total_goals") {
      period_num <- as.numeric(gsub("[^0-9]", "", market_info$period))
      
      if (period_num <= length(period_data$periods)) {
        period <- period_data$periods[[period_num]]
        total_goals <- (period$home_score %||% 0) + (period$away_score %||% 0)
        
        result$period <- period_num
        result$total_goals <- total_goals
        result$home_score <- period$home_score %||% 0
        result$away_score <- period$away_score %||% 0
        
        if (!is.null(line_value)) {
          line_val_num <- as.numeric(line_value)
          if (grepl("over", market_lower)) {
            result$bet_won <- total_goals > line_val_num
          } else if (grepl("under", market_lower)) {
            result$bet_won <- total_goals < line_val_num
          }
          result$line_value <- line_val_num
        }
      } else {
        # SAFETY FALLBACK - return empty result instead of crashing
        debug_cat(sprintf("  ⚠️ Period %d not available (only %d periods)\n", 
                          period_num, length(period_data$periods)))
        result$period <- period_num
        result$total_goals <- 0
        result$error <- "Period data not available"
      }
      
    } else if (market_info$type == "period_both_teams_score") {
      period_num <- as.numeric(gsub("[^0-9]", "", market_info$period))
      
      if (period_num <= length(period_data$periods)) {
        period <- period_data$periods[[period_num]]
        both_scored <- ((period$home_score %||% 0) > 0 && (period$away_score %||% 0) > 0)
        
        result$period <- period_num
        result$both_teams_scored <- both_scored
        result$home_score <- period$home_score %||% 0
        result$away_score <- period$away_score %||% 0
        result$bet_won <- both_scored  # Assuming "Yes" bet
      } else {
        debug_cat(sprintf("  ⚠️ Period %d not available\n", period_num))
        result$period <- period_num
        result$both_teams_scored <- FALSE
        result$error <- "Period data not available"
      }
      
    } else if (market_info$type == "period_moneyline") {
      period_num <- as.numeric(gsub("[^0-9]", "", market_info$period))
      
      if (period_num <= length(period_data$periods)) {
        period <- period_data$periods[[period_num]]
        
        result$period <- period_num
        result$home_score <- period$home_score %||% 0
        result$away_score <- period$away_score %||% 0
        
        if (result$home_score > result$away_score) {
          result$winner <- "home"
        } else if (result$away_score > result$home_score) {
          result$winner <- "away"
        } else {
          result$winner <- "tie"
          # For 3-way moneyline, a tie (draw) is a valid outcome
          if (!is.null(market_info$bet_type) && market_info$bet_type == "3way") {
            result$bet_won <- TRUE  # Draw bet wins
            debug_cat(sprintf("  ✓ 3-way moneyline DRAW! Bet won.\n"))
          }
        }
      } else {
        debug_cat(sprintf("  ⚠️ Period %d not available\n", period_num))
        result$period <- period_num
        result$winner <- "unknown"
        result$error <- "Period data not available"
      }
    }
    
    return(result)
    
  }, error = function(e) {
    debug_cat(sprintf("  ERROR in resolve_nhl_period_market: %s\n", e$message))
    return(list(success = FALSE, error = paste("Error:", e$message)))
  })
}

# Resolve NHL player period markets
resolve_nhl_player_period_market <- function(game_id, player_name, market_info, market_lower, line_value = NULL) {
  debug_cat(sprintf("  Resolving NHL player period market for %s\n", player_name))
  
  tryCatch({
    period_num <- as.numeric(gsub("[^0-9]", "", market_info$period))
    
    player_period_data <- get_espn_nhl_player_period_stats(game_id, player_name, period_num)
    
    if (!player_period_data$success) {
      return(list(success = FALSE, error = player_period_data$error))
    }
    
    result <- list(
      success = TRUE,
      player = player_name,
      period = period_num
    )
    
    # Extract the specific stat
    if (market_info$stat == "goals") {
      result$actual_value <- player_period_data$goals
      result$stat_name <- "goals"
    } else if (market_info$stat == "assists") {
      result$actual_value <- player_period_data$assists
      result$stat_name <- "assists"
    } else if (market_info$stat == "points") {
      result$actual_value <- player_period_data$points
      result$stat_name <- "points"
    } else if (market_info$stat == "shots") {
      result$actual_value <- player_period_data$shots
      result$stat_name <- "shots"
    } else if (market_info$stat == "saves") {
      result$actual_value <- player_period_data$saves
      result$stat_name <- "saves"
    }
    
    # Check against line value
    if (!is.null(line_value)) {
      line_val_num <- as.numeric(line_value)
      if (grepl("over", market_lower)) {
        result$bet_won <- result$actual_value > line_val_num
      } else if (grepl("under", market_lower)) {
        result$bet_won <- result$actual_value < line_val_num
      }
      result$line_value <- line_val_num
    }
    
    return(result)
    
  }, error = function(e) {
    return(list(success = FALSE, error = paste("Error:", e$message)))
  })
}

# Find NHL game ID from ESPN (with timezone handling)
find_nhl_game_id <- function(home_team, away_team, game_date) {
  debug_cat(sprintf("\nSearching for NHL game: %s @ %s on %s\n", away_team, home_team, game_date))
  
  # First try exact date
  debug_cat("  1. Trying exact date first...\n")
  game_id <- find_nhl_game_id_single_date(home_team, away_team, game_date)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on exact date! Game ID: %s\n", game_id))
    return(game_id)
  }
  
  # If not found, try previous day (for timezone issues)
  prev_date <- as.Date(game_date) - 1
  debug_cat(sprintf("  2. Trying previous day %s (for timezone issues)...\n", prev_date))
  game_id <- find_nhl_game_id_single_date(home_team, away_team, prev_date)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on previous day! Game ID: %s\n", game_id))
    debug_cat("    ⚠️ Timezone issue detected: Game appears 1 day earlier in ESPN\n")
    debug_cat("    This happens when games start after 8 PM Eastern\n")
    return(game_id)
  }
  
  # If still not found, try next day
  next_date <- as.Date(game_date) + 1
  debug_cat(sprintf("  3. Trying next day %s...\n", next_date))
  game_id <- find_nhl_game_id_single_date(home_team, away_team, next_date)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on next day! Game ID: %s\n", game_id))
    debug_cat("    ⚠️ Game appears 1 day later in ESPN\n")
    return(game_id)
  }
  
  # If still not found, use wider search (±3 days)
  debug_cat("  4. Not found with timezone fix, using wider search (±3 days)...\n")
  return(find_nhl_game_id_flexible(home_team, away_team, game_date, max_days_range = 3))
}

# Helper function for single date
find_nhl_game_id_single_date <- function(home_team, away_team, game_date) {
  tryCatch({
    games <- get_espn_nhl_games(game_date)
    
    if (is.null(games) || nrow(games) == 0) {
      return(NA)
    }
    
    # Clean all names
    clean_team <- function(name) {
      name <- tolower(trimws(name))
      name <- gsub("[[:punct:]]", "", name)
      name <- gsub("\\s+", " ", name)
      name
    }
    
    home_clean <- clean_team(home_team)
    away_clean <- clean_team(away_team)
    
    for (i in 1:nrow(games)) {
      game <- games[i, ]
      game_id <- game$id
      game_name <- game$name
      
      # Clean the game name
      game_name_clean <- clean_team(game_name)
      
      # Check if our teams appear in the game name
      home_in_name <- grepl(home_clean, game_name_clean, ignore.case = TRUE)
      away_in_name <- grepl(away_clean, game_name_clean, ignore.case = TRUE)
      
      if (home_in_name && away_in_name) {
        debug_cat(sprintf("    Found match on %s: %s (ID: %s)\n", game_date, game_name, game_id))
        return(game_id)
      }
    }
    
    return(NA)
  }, error = function(e) {
    return(NA)
  })
}

# Flexible search for wider date range
find_nhl_game_id_flexible <- function(home_team, away_team, game_date, max_days_range = 7) {
  base_date <- as.Date(game_date)
  
  # Clean team names for matching
  clean_team <- function(name) {
    name <- tolower(trimws(name))
    name <- gsub("[[:punct:]]", "", name)
    name <- gsub("\\s+", " ", name)
    name
  }
  
  home_clean <- clean_team(home_team)
  away_clean <- clean_team(away_team)
  
  debug_cat(sprintf("  Searching ±%d days for '%s' @ '%s'\n",
                    max_days_range, away_clean, home_clean))
  
  # Try dates in order: exact, -1, +1, -2, +2, etc.
  date_offsets <- c(0)
  for (i in 1:max_days_range) {
    date_offsets <- c(date_offsets, -i, i)
  }
  
  for (offset in date_offsets) {
    current_date <- base_date + offset
    debug_cat(sprintf("    Checking %s (offset: %+d)... ", current_date, offset))
    
    game_id <- find_nhl_game_id_single_date(home_team, away_team, current_date)
    
    if (!is.na(game_id)) {
      debug_cat(sprintf("FOUND!\n"))
      debug_cat(sprintf("    Game found with %+d day offset\n", offset))
      return(game_id)
    } else {
      debug_cat("no\n")
    }
  }
  
  debug_cat("  No game found in date range\n")
  return(NA)
}

# ==============================================
# ROBUST NHL GAME ID FINDER WITH BETTER ERROR HANDLING
# ==============================================

find_nhl_game_id_robust <- function(home_team, away_team, game_date) {
  debug_cat(sprintf("\n🔍 NHL ROBUST SEARCH: %s @ %s on %s\n", 
                    away_team, home_team, game_date))
  
  # Try exact date first
  game_id <- find_nhl_game_id_single_date(home_team, away_team, game_date)
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on exact date: %s\n", game_id))
    return(game_id)
  }
  
  # Try +/- 1 day for timezone issues
  for (offset in c(-1, 1)) {
    adj_date <- as.Date(game_date) + offset
    debug_cat(sprintf("  Trying %s (offset %+d)...\n", adj_date, offset))
    game_id <- find_nhl_game_id_single_date(home_team, away_team, adj_date)
    if (!is.na(game_id)) {
      debug_cat(sprintf("  ✓ Found on %s (offset %+d): %s\n", adj_date, offset, game_id))
      return(game_id)
    }
  }
  
  # Try generic search without team names
  debug_cat("  Trying generic search (any game on date)...\n")
  espn_date <- format(as.Date(game_date), "%Y%m%d")
  url <- paste0("http://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates=", espn_date)
  
  tryCatch({
    response <- GET(url, timeout = 10)
    if (status_code(response) == 200) {
      data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
      
      if (!is.null(data$events) && length(data$events) > 0) {
        # Handle both data frame and list cases
        num_games <- if (is.data.frame(data$events)) nrow(data$events) else length(data$events)
        
        if (num_games > 0) {
          # Get the first game ID
          first_game_id <- if (is.data.frame(data$events)) {
            data$events$id[1]
          } else {
            data$events[[1]]$id
          }
          
          debug_cat(sprintf("  Found %d games, using first: %s\n", num_games, first_game_id))
          return(first_game_id)
        }
      }
    }
  }, error = function(e) {
    debug_cat(sprintf("  Generic search error: %s\n", e$message))
  })
  
  # Try wider date range as last resort
  debug_cat("  Trying wider date range (±3 days)...\n")
  for (offset in c(-2, 2, -3, 3)) {
    adj_date <- as.Date(game_date) + offset
    game_id <- find_nhl_game_id_single_date(home_team, away_team, adj_date)
    if (!is.na(game_id)) {
      debug_cat(sprintf("  ✓ Found on %s (offset %+d): %s\n", adj_date, offset, game_id))
      return(game_id)
    }
  }
  
  debug_cat("  ✗ No NHL game found\n")
  return(NA)
}

# ==============================================
# MLB FUNCTIONS
# ==============================================

# Get MLB games from ESPN API for a specific date
get_espn_mlb_games <- function(game_date) {
  tryCatch({
    espn_date <- format(as.Date(game_date), "%Y%m%d")
    url <- paste0("http://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates=", espn_date)
    
    debug_cat(sprintf("  Calling ESPN MLB API for games on %s\n", espn_date))
    response <- GET(url, timeout = 10)
    
    if (status_code(response) != 200) {
      debug_cat(sprintf("  ESPN MLB API returned status: %d\n", status_code(response)))
      return(NULL)
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    if (is.null(data$events) || nrow(data$events) == 0) {
      debug_cat("  No MLB events found\n")
      return(NULL)
    }
    
    debug_cat(sprintf("  ESPN MLB API returned %d games\n", nrow(data$events)))
    return(data$events)
    
  }, error = function(e) {
    debug_cat(sprintf("  ESPN MLB API error: %s\n", e$message))
    return(NULL)
  })
}

# Find MLB game ID
find_mlb_game_id <- function(home_team, away_team, game_date) {
  debug_cat(sprintf("\nSearching for MLB game: %s @ %s on %s\n", away_team, home_team, game_date))
  
  # Try exact date first
  debug_cat("  1. Trying exact date first...\n")
  game_id <- find_mlb_game_id_single_date(home_team, away_team, game_date)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on exact date! Game ID: %s\n", game_id))
    return(game_id)
  }
  
  # If not found, try previous day (for timezone issues)
  prev_date <- as.Date(game_date) - 1
  debug_cat(sprintf("  2. Trying previous day %s (for timezone issues)...\n", prev_date))
  game_id <- find_mlb_game_id_single_date(home_team, away_team, prev_date)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on previous day! Game ID: %s\n", game_id))
    debug_cat("    ⚠️ Timezone issue detected: Game appears 1 day earlier in ESPN\n")
    return(game_id)
  }
  
  # If still not found, try next day
  next_date <- as.Date(game_date) + 1
  debug_cat(sprintf("  3. Trying next day %s...\n", next_date))
  game_id <- find_mlb_game_id_single_date(home_team, away_team, next_date)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on next day! Game ID: %s\n", game_id))
    return(game_id)
  }
  
  # Use wider search as last resort
  debug_cat("  4. Using wider search...\n")
  return(find_mlb_game_id_flexible(home_team, away_team, game_date, max_days_range = 3))
}

find_mlb_game_id_single_date <- function(home_team, away_team, game_date) {
  tryCatch({
    games <- get_espn_mlb_games(game_date)

    if (is.null(games) || nrow(games) == 0) {
      return(NA)
    }

    clean_team <- function(name) {
      if (is.null(name) || is.na(name)) return("")
      name <- tolower(name)
      name <- gsub("st\\.", "st", name)
      name <- gsub("[^a-z0-9 ]", "", name)
      name <- gsub("\\s+", " ", trimws(name))
      name
    }

    get_team_keywords <- function(name) {
      name <- clean_team(name)
      parts <- unlist(strsplit(name, " "))
      parts <- parts[nchar(parts) >= 3]
      parts
    }

    home_clean <- clean_team(home_team)
    away_clean <- clean_team(away_team)
    home_keywords <- get_team_keywords(home_team)
    away_keywords <- get_team_keywords(away_team)

    for (i in 1:nrow(games)) {
      game <- games[i, ]
      game_id <- game$id
      game_name <- tolower(game$name)
      game_name_clean <- clean_team(game_name)

      home_stripped <- gsub("[^a-z0-9]", "", home_clean)
      away_stripped <- gsub("[^a-z0-9]", "", away_clean)
      game_stripped <- gsub("[^a-z0-9]", "", game_name_clean)

      if (grepl(home_stripped, game_stripped) && grepl(away_stripped, game_stripped)) {
        debug_cat(sprintf("    Found match (exact): %s (ID: %s)\n", game$name, game_id))
        return(game_id)
      }

      home_match <- any(sapply(home_keywords, function(kw) grepl(kw, game_name_clean)))
      away_match <- any(sapply(away_keywords, function(kw) grepl(kw, game_name_clean)))
      if (home_match && away_match) {
        debug_cat(sprintf("    Found match (keyword): %s (ID: %s)\n", game$name, game_id))
        return(game_id)
      }
    }

    if (length(home_keywords) > 0 || length(away_keywords) > 0) {
      for (i in 1:nrow(games)) {
        game <- games[i, ]
        game_id <- game$id
        game_name_clean <- clean_team(tolower(game$name))
        home_match <- any(sapply(home_keywords, function(kw) grepl(kw, game_name_clean)))
        away_match <- any(sapply(away_keywords, function(kw) grepl(kw, game_name_clean)))
        if (home_match || away_match) {
          debug_cat(sprintf("    Found partial match: %s (ID: %s)\n", game$name, game_id))
          return(game_id)
        }
      }
    }

    return(NA)

  }, error = function(e) {
    return(NA)
  })
}

find_mlb_game_id_flexible <- function(home_team, away_team, game_date, max_days_range = 7) {
  base_date <- as.Date(game_date)
  
  # Try dates in order: exact, -1, +1, -2, +2, etc.
  date_offsets <- c(0)
  for (i in 1:max_days_range) {
    date_offsets <- c(date_offsets, -i, i)
  }
  
  for (offset in date_offsets) {
    current_date <- base_date + offset
    debug_cat(sprintf("    Checking %s (offset: %+d)... ", current_date, offset))
    
    game_id <- find_mlb_game_id_single_date(home_team, away_team, current_date)
    
    if (!is.na(game_id)) {
      debug_cat(sprintf("FOUND!\n"))
      return(game_id)
    } else {
      debug_cat("no\n")
    }
  }
  
  debug_cat("  No MLB game found in date range\n")
  return(NA)
}

# Get MLB player stats from ESPN - UPDATED to handle both batting and pitching
get_espn_mlb_player_stats <- function(game_id, player_name) {
  tryCatch({
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary?event=", game_id)
    debug_cat(sprintf("  Calling ESPN MLB API for player stats: %s
", url))

    response <- GET(url, timeout = 10)
    if (status_code(response) != 200) {
      debug_cat(sprintf("  ESPN MLB API returned status: %d
", status_code(response)))
      return(NULL)
    }

    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)

    # Helper to clean names for matching
    clean_name_for_matching <- function(name) {
      name <- tolower(trimws(name))
      name <- gsub("[[:punct:]]", "", name)
      name <- gsub("\\s+", " ", name)
      name
    }

    target_clean <- clean_name_for_matching(player_name)
    debug_cat(sprintf("  Searching for MLB player: %s
", target_clean))

    # Advanced fuzzy matching logic
    is_player_match <- function(target, athlete) {
      t_clean <- clean_name_for_matching(target)
      a_clean <- clean_name_for_matching(athlete)

      if (t_clean == a_clean) return(TRUE)
      if (grepl(t_clean, a_clean, fixed = TRUE) || grepl(a_clean, t_clean, fixed = TRUE)) return(TRUE)

      t_parts <- strsplit(t_clean, " ")[[1]]
      a_parts <- strsplit(a_clean, " ")[[1]]

      if (length(t_parts) >= 2 && length(a_parts) >= 2) {
        t_last <- t_parts[length(t_parts)]
        a_last <- a_parts[length(a_parts)]

        # If last names match exactly or very closely
        if (t_last == a_last || utils::adist(t_last, a_last)[1,1] <= 1) {
          t_first <- t_parts[1]
          a_first <- a_parts[1]

          # Check initials
          if (nchar(t_first) == 1 && substr(a_first, 1, 1) == t_first) return(TRUE)
          if (nchar(a_first) == 1 && substr(t_first, 1, 1) == a_first) return(TRUE)

          # Allow slight typo in first name (distance <= 2)
          if (utils::adist(t_first, a_first)[1,1] <= 2) return(TRUE)
        }

        # Full name fuzzy match (allow distance up to 2)
        if (utils::adist(t_clean, a_clean)[1,1] <= 2) return(TRUE)
      }
      return(FALSE)
    }

    # Initialize result structure
    result <- list(
      player = player_name, # will be updated to actual name
      team = "Unknown",
      # Hitting stats
      at_bats = 0, runs = 0, hits = 0, rbi = 0, walks = 0, strikeouts = 0,
      home_runs = 0, stolen_bases = 0, doubles = 0, triples = 0, singles = 0, total_bases = 0,
      # Pitching stats
      innings_pitched = 0, outs_recorded = 0, hits_allowed = 0, runs_allowed = 0,
      earned_runs = 0, walks_allowed = 0, pitching_strikeouts = 0, home_runs_allowed = 0,
      pitches_thrown = 0, era = 0
    )

    found_player <- FALSE

    # Check for boxscore players
    if (!is.null(data$boxscore) && !is.null(data$boxscore$players)) {
      players_df <- data$boxscore$players

      for (team_idx in 1:nrow(players_df)) {
        team_data <- players_df[team_idx, ]
        team_name <- team_data$team$displayName %||% "Unknown"

        if (!is.null(team_data$statistics) && length(team_data$statistics) > 0) {
          stats_list <- team_data$statistics[[1]]

          if (is.data.frame(stats_list) && nrow(stats_list) > 0) {
            # Iterate through all stat groups (batting and pitching)
            for (stat_idx in 1:nrow(stats_list)) {
              stat_type <- stats_list$type[stat_idx] %||% "unknown"

              if (stat_type %in% c("batting", "pitching")) {
                athletes_data <- stats_list$athletes[stat_idx]
                if (is.list(athletes_data) && length(athletes_data) > 0) {
                  athletes_df <- athletes_data[[1]]

                  if (is.data.frame(athletes_df) && nrow(athletes_df) > 0) {
                    for (athlete_idx in 1:nrow(athletes_df)) {
                      athlete_row <- athletes_df[athlete_idx, ]

                      # Extract name safely
                      athlete_name <- NULL
                      if (!is.null(athlete_row$athlete) && is.data.frame(athlete_row$athlete)) {
                        if ("displayName" %in% colnames(athlete_row$athlete)) athlete_name <- athlete_row$athlete$displayName[1]
                      }
                      if (is.null(athlete_name) && "athlete.displayName" %in% colnames(athlete_row)) athlete_name <- athlete_row$athlete.displayName
                      if (is.null(athlete_name) && "displayName" %in% colnames(athlete_row)) athlete_name <- athlete_row$displayName

                      if (!is.null(athlete_name) && is_player_match(player_name, athlete_name)) {
                        found_player <- TRUE
                        result$player <- athlete_name
                        result$team <- team_name
                        debug_cat(sprintf("  Found MLB player match in %s stats: %s
", stat_type, athlete_name))

                        stats_vector <- NULL
                        if (!is.null(athlete_row$stats) && is.list(athlete_row$stats) && length(athlete_row$stats) > 0) {
                          stats_vector <- athlete_row$stats[[1]]
                        }

                        if (!is.null(stats_vector)) {
                          if (stat_type == "batting" && length(stats_vector) >= 7) {
                            # Parse H-AB
                            hit_ab <- strsplit(as.character(stats_vector[1]), "-")[[1]]
                            if (length(hit_ab) == 2) {
                              result$hits <- as.numeric(hit_ab[1]) %||% 0
                              result$at_bats <- as.numeric(hit_ab[2]) %||% 0
                            }
                            if (length(stats_vector) >= 2) result$runs <- as.numeric(stats_vector[2]) %||% 0
                            if (result$hits == 0 && length(stats_vector) >= 3) result$hits <- as.numeric(stats_vector[3]) %||% 0
                            if (length(stats_vector) >= 4) result$doubles <- as.numeric(stats_vector[4]) %||% 0
                            if (length(stats_vector) >= 5) result$triples <- as.numeric(stats_vector[5]) %||% 0
                            if (length(stats_vector) >= 6) result$home_runs <- as.numeric(stats_vector[6]) %||% 0
                            if (length(stats_vector) >= 7) result$rbi <- as.numeric(stats_vector[7]) %||% 0
                            if (length(stats_vector) >= 8) result$walks <- as.numeric(stats_vector[8]) %||% 0
                            if (length(stats_vector) >= 9) result$strikeouts <- as.numeric(stats_vector[9]) %||% 0

                            result$singles <- max(0, result$hits - (result$doubles + result$triples + result$home_runs))
                            result$total_bases <- result$singles + (result$doubles * 2) + (result$triples * 3) + (result$home_runs * 4)

                          } else if (stat_type == "pitching" && length(stats_vector) >= 9) {
                            # [1] IP, [2] H, [3] R, [4] ER, [5] BB, [6] K, [7] HR, [8] PC-ST, [9] ERA
                            ip_str <- as.character(stats_vector[1])
                            result$innings_pitched <- as.numeric(ip_str) %||% 0

                            # Calculate outs from IP (e.g., 5.1 -> 16 outs)
                            ip_parts <- strsplit(ip_str, "\\.")[[1]]
                            full_innings <- as.numeric(ip_parts[1]) %||% 0
                            partial_outs <- if(length(ip_parts) > 1) as.numeric(ip_parts[2]) %||% 0 else 0
                            result$outs_recorded <- (full_innings * 3) + partial_outs

                            result$hits_allowed <- as.numeric(stats_vector[2]) %||% 0
                            result$runs_allowed <- as.numeric(stats_vector[3]) %||% 0
                            result$earned_runs <- as.numeric(stats_vector[4]) %||% 0
                            result$walks_allowed <- as.numeric(stats_vector[5]) %||% 0
                            result$pitching_strikeouts <- as.numeric(stats_vector[6]) %||% 0
                            result$home_runs_allowed <- as.numeric(stats_vector[7]) %||% 0

                            pc_st <- strsplit(as.character(stats_vector[8]), "-")[[1]]
                            if (length(pc_st) >= 1) result$pitches_thrown <- as.numeric(pc_st[1]) %||% 0

                            result$era <- as.numeric(stats_vector[9]) %||% 0
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    if (found_player) {
      return(result)
    }

    debug_cat("  MLB player not found in boxscore
")
    return(NULL)

  }, error = function(e) {
    debug_cat(sprintf("  Error getting MLB player stats: %s
", e$message))
    return(NULL)
  })
}

# Fetch MLB player stats (main function) - UPDATED with pitching stats
fetch_mlb_player_stats <- function(player_name, season, game_date = NULL, game_id = NULL) {
  debug_cat(sprintf("\nDEBUG fetch_mlb_player_stats:\n"))
  debug_cat(sprintf("  Player: %s\n", player_name))
  debug_cat(sprintf("  Season: %s\n", season))
  debug_cat(sprintf("  Game ID: %s\n", game_id))
  
  tryCatch({
    if (!is.null(game_id) && !is.na(game_id) && game_id != "NA") {
      debug_cat("  Using ESPN API for MLB player stats...\n")
      player_stats <- get_espn_mlb_player_stats(game_id, player_name)
      
      if (!is.null(player_stats)) {
        debug_cat(sprintf("  Found player stats via ESPN API (type: %s)\n", player_stats$stat_type %||% "unknown"))

        stats_list <- list()

        stat_fields <- c("at_bats", "runs", "hits", "rbi", "walks", "strikeouts",
                         "home_runs", "stolen_bases", "doubles", "triples",
                         "singles", "total_bases",
                         "innings_pitched", "outs_recorded", "hits_allowed",
                         "runs_allowed", "earned_runs", "walks_allowed",
                         "pitching_strikeouts", "home_runs_allowed",
                         "pitches_thrown", "era")
        for (field in stat_fields) {
          val <- player_stats[[field]]
          if (!is.null(val)) stats_list[[field]] <- val
        }

        return(list(
          found = TRUE,
          player = player_stats$player,
          games = 1,
          stats = stats_list
        ))
      } else {
        debug_cat("  ESPN API returned NULL player stats\n")
        return(list(found = FALSE, error = "ESPN API returned no player stats for this game"))
      }
    }
    
    debug_cat("ERROR: No game_id provided for ESPN API lookup\n")
    return(list(found = FALSE, error = "No game_id available for ESPN API"))
    
  }, error = function(e) {
    debug_cat(sprintf("ERROR in fetch_mlb_player_stats: %s\n", e$message))
    return(list(found = FALSE, error = paste("Error fetching MLB stats:", e$message)))
  })
}

# ==============================================
# WNBA FUNCTIONS
# ==============================================

# Get WNBA game from ESPN API
get_espn_wnba_games <- function(game_date) {
  tryCatch({
    espn_date <- format(as.Date(game_date), "%Y%m%d")
    url <- paste0("http://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard?dates=", espn_date)
    
    debug_cat(sprintf("  Calling ESPN WNBA API for games on %s\n", espn_date))
    response <- GET(url, timeout = 10)
    
    if (status_code(response) != 200) {
      debug_cat(sprintf("  ESPN WNBA API returned status: %d\n", status_code(response)))
      return(NULL)
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    if (is.null(data$events) || nrow(data$events) == 0) {
      debug_cat("  No WNBA events found\n")
      return(NULL)
    }
    
    debug_cat(sprintf("  ESPN WNBA API returned %d games\n", nrow(data$events)))
    return(data$events)
    
  }, error = function(e) {
    debug_cat(sprintf("  ESPN WNBA API error: %s\n", e$message))
    return(NULL)
  })
}

# Find WNBA game ID
find_wnba_game_id <- function(home_team, away_team, game_date) {
  debug_cat(sprintf("\nSearching for WNBA game: %s @ %s on %s\n", away_team, home_team, game_date))
  
  # Try exact date first
  debug_cat("  1. Trying exact date first...\n")
  game_id <- find_wnba_game_id_single_date(home_team, away_team, game_date)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on exact date! Game ID: %s\n", game_id))
    return(game_id)
  }
  
  # If not found, try previous day (for timezone issues)
  prev_date <- as.Date(game_date) - 1
  debug_cat(sprintf("  2. Trying previous day %s (for timezone issues)...\n", prev_date))
  game_id <- find_wnba_game_id_single_date(home_team, away_team, prev_date)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on previous day! Game ID: %s\n", game_id))
    return(game_id)
  }
  
  # If still not found, try next day
  next_date <- as.Date(game_date) + 1
  debug_cat(sprintf("  3. Trying next day %s...\n", next_date))
  game_id <- find_wnba_game_id_single_date(home_team, away_team, next_date)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on next day! Game ID: %s\n", game_id))
    return(game_id)
  }
  
  # Use wider search as last resort
  debug_cat("  4. Using wider search...\n")
  return(find_wnba_game_id_flexible(home_team, away_team, game_date, max_days_range = 3))
}

find_wnba_game_id_single_date <- function(home_team, away_team, game_date) {
  tryCatch({
    games <- get_espn_wnba_games(game_date)
    
    if (is.null(games) || nrow(games) == 0) {
      return(NA)
    }
    
    # Clean team names for matching
    clean_team <- function(name) {
      if (is.null(name) || is.na(name)) return("")
      name <- tolower(name)
      name <- gsub("[^a-z0-9]", "", name)
      name
    }
    
    home_clean <- clean_team(home_team)
    away_clean <- clean_team(away_team)
    
    for (i in 1:nrow(games)) {
      game <- games[i, ]
      game_id <- game$id
      game_name <- tolower(game$name)
      
      # Clean game name for matching
      game_name_clean <- clean_team(game_name)
      
      # Check if both teams appear in the game name
      if (grepl(home_clean, game_name_clean) && grepl(away_clean, game_name_clean)) {
        debug_cat(sprintf("    Found match: %s (ID: %s)\n", game$name, game_id))
        return(game_id)
      }
    }
    
    return(NA)
    
  }, error = function(e) {
    return(NA)
  })
}

find_wnba_game_id_flexible <- function(home_team, away_team, game_date, max_days_range = 7) {
  base_date <- as.Date(game_date)
  
  date_offsets <- c(0)
  for (i in 1:max_days_range) {
    date_offsets <- c(date_offsets, -i, i)
  }
  
  for (offset in date_offsets) {
    current_date <- base_date + offset
    debug_cat(sprintf("    Checking %s (offset: %+d)... ", current_date, offset))
    
    game_id <- find_wnba_game_id_single_date(home_team, away_team, current_date)
    
    if (!is.na(game_id)) {
      debug_cat(sprintf("FOUND!\n"))
      return(game_id)
    } else {
      debug_cat("no\n")
    }
  }
  
  debug_cat("  No WNBA game found in date range\n")
  return(NA)
}

# Get WNBA player stats from ESPN (similar to NBA but with WNBA endpoint)
get_espn_wnba_player_stats <- function(game_id, player_name) {
  tryCatch({
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/summary?event=", game_id)
    debug_cat(sprintf("  Calling ESPN WNBA API for player stats: %s\n", url))
    
    response <- GET(url, timeout = 10)
    if (status_code(response) != 200) {
      debug_cat(sprintf("  ESPN WNBA API returned status: %d\n", status_code(response)))
      return(NULL)
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    # Reuse NBA extraction logic with WNBA data
    return(get_espn_basketball_player_stats_from_data(data, player_name))
    
  }, error = function(e) {
    debug_cat(sprintf("  Error getting WNBA player stats: %s\n", e$message))
    return(NULL)
  })
}

# Helper function for basketball stats extraction (works for NBA/WNBA/NCAA)
get_espn_basketball_player_stats_from_data <- function(data, player_name) {
  tryCatch({
    if (is.null(data$boxscore) || is.null(data$boxscore$players)) {
      return(NULL)
    }
    
    players_df <- data$boxscore$players
    
    # Clean name for matching
    clean_name_for_matching <- function(name) {
      name <- tolower(trimws(name))
      name <- gsub("[[:punct:]]", "", name)
      name <- gsub("\\s+", " ", name)
      name
    }
    
    target_name <- clean_name_for_matching(player_name)
    
    # Search through all teams
    for (team_idx in 1:nrow(players_df)) {
      team_stats <- players_df$statistics[[team_idx]]
      
      if (!is.data.frame(team_stats) || !"athletes" %in% names(team_stats)) {
        next
      }
      
      athletes_list <- team_stats$athletes
      if (!is.list(athletes_list) || length(athletes_list) == 0) {
        next
      }
      
      if (is.list(athletes_list[[1]])) {
        athletes_df <- athletes_list[[1]]
      } else {
        athletes_df <- athletes_list
      }
      
      if (!is.data.frame(athletes_df) || nrow(athletes_df) == 0) {
        next
      }
      
      for (athlete_idx in 1:nrow(athletes_df)) {
        athlete_row <- athletes_df[athlete_idx, ]
        
        # Extract player name
        current_player_name <- NULL
        
        if (!is.null(athlete_row$athlete) && is.data.frame(athlete_row$athlete)) {
          if ("displayName" %in% colnames(athlete_row$athlete)) {
            current_player_name <- athlete_row$athlete$displayName[1]
          }
        }
        
        if (is.null(current_player_name) && "displayName" %in% colnames(athlete_row)) {
          current_player_name <- athlete_row$displayName
        }
        
        if (is.null(current_player_name) && "athlete.displayName" %in% colnames(athlete_row)) {
          current_player_name <- athlete_row$athlete.displayName
        }
        
        if (!is.null(current_player_name)) {
          current_name_clean <- clean_name_for_matching(current_player_name)
          
          if (current_name_clean == target_name ||
              grepl(target_name, current_name_clean, fixed = TRUE) ||
              grepl(current_name_clean, target_name, fixed = TRUE)) {
            
            debug_cat(sprintf("  Found match: %s\n", current_player_name))
            
            # Extract stats (ESPN format)
            stats_list <- list(
              player = current_player_name,
              points = 0,
              rebounds = 0,
              assists = 0,
              steals = 0,
              blocks = 0,
              turnovers = 0,
              minutes = 0,
              field_goals_made = 0,
              field_goals_attempted = 0,
              three_pointers_made = 0,
              free_throws_made = 0
            )
            
            if (!is.null(athlete_row$stats)) {
              stats_data <- athlete_row$stats
              stats_vector <- NULL
              
              if (is.list(stats_data) && length(stats_data) > 0) {
                stats_vector <- stats_data[[1]]
              }
              
              if (!is.null(stats_vector) && length(stats_vector) >= 10) {
                # ESPN stats format
                stats_list$minutes <- safe_numeric(stats_vector[1])
                stats_list$points <- safe_numeric(stats_vector[2])
                stats_list$rebounds <- safe_numeric(stats_vector[6])
                stats_list$assists <- safe_numeric(stats_vector[7])
                stats_list$steals <- safe_numeric(stats_vector[8])
                stats_list$blocks <- safe_numeric(stats_vector[9])
                stats_list$turnovers <- safe_numeric(stats_vector[10])
                
                if (length(stats_vector) >= 3 && grepl("-", stats_vector[3])) {
                  fg_parts <- strsplit(stats_vector[3], "-")[[1]]
                  stats_list$field_goals_made <- safe_numeric(fg_parts[1])
                  stats_list$field_goals_attempted <- safe_numeric(fg_parts[2])
                }
                
                if (length(stats_vector) >= 4 && grepl("-", stats_vector[4])) {
                  three_pt_parts <- strsplit(stats_vector[4], "-")[[1]]
                  stats_list$three_pointers_made <- safe_numeric(three_pt_parts[1])
                }
                
                debug_cat(sprintf("    Stats: %d pts, %d reb, %d ast\n",
                                  stats_list$points, stats_list$rebounds, stats_list$assists))
              }
            }
            
            return(stats_list)
          }
        }
      }
    }
    
    return(NULL)
    
  }, error = function(e) {
    debug_cat(sprintf("  Error extracting player stats: %s\n", e$message))
    return(NULL)
  })
}

# Fetch WNBA player stats
fetch_wnba_player_stats <- function(player_name, season, game_date = NULL, game_id = NULL) {
  debug_cat(sprintf("\nDEBUG fetch_wnba_player_stats:\n"))
  debug_cat(sprintf("  Player: %s\n", player_name))
  debug_cat(sprintf("  Season: %s\n", season))
  debug_cat(sprintf("  Game ID: %s\n", game_id))
  
  tryCatch({
    if (!is.null(game_id) && !is.na(game_id) && game_id != "NA") {
      debug_cat("  Using ESPN API for WNBA player stats...\n")
      player_stats <- get_espn_wnba_player_stats(game_id, player_name)
      
      if (!is.null(player_stats)) {
        return(list(
          found = TRUE,
          player = player_stats$player,
          games = 1,
          stats = list(
            points = player_stats$points,
            rebounds = player_stats$rebounds,
            assists = player_stats$assists,
            steals = player_stats$steals,
            blocks = player_stats$blocks,
            turnovers = player_stats$turnovers,
            three_pointers_made = player_stats$three_pointers_made,
            field_goals_made = player_stats$field_goals_made,
            minutes = player_stats$minutes
          )
        ))
      } else {
        return(list(found = FALSE, error = "ESPN API returned no player stats"))
      }
    }
    
    return(list(found = FALSE, error = "No game_id available"))
    
  }, error = function(e) {
    return(list(found = FALSE, error = paste("Error:", e$message)))
  })
}

# ==============================================
# NCAA FOOTBALL FUNCTIONS
# ==============================================

# Get NCAAF game from ESPN API
get_espn_ncaaf_games <- function(game_date) {
  tryCatch({
    espn_date <- format(as.Date(game_date), "%Y%m%d")
    url <- paste0("http://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates=", espn_date)
    
    debug_cat(sprintf("  Calling ESPN NCAAF API for games on %s\n", espn_date))
    response <- GET(url, timeout = 10)
    
    if (status_code(response) != 200) {
      debug_cat(sprintf("  ESPN NCAAF API returned status: %d\n", status_code(response)))
      return(NULL)
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    if (is.null(data$events) || nrow(data$events) == 0) {
      debug_cat("  No NCAAF events found\n")
      return(NULL)
    }
    
    debug_cat(sprintf("  ESPN NCAAF API returned %d games\n", nrow(data$events)))
    return(data$events)
    
  }, error = function(e) {
    debug_cat(sprintf("  ESPN NCAAF API error: %s\n", e$message))
    return(NULL)
  })
}

# Find NCAAF game ID
find_ncaaf_game_id <- function(home_team, away_team, game_date) {
  debug_cat(sprintf("\nSearching for NCAAF game: %s @ %s on %s\n", away_team, home_team, game_date))
  
  # Try exact date first
  debug_cat("  1. Trying exact date first...\n")
  game_id <- find_ncaaf_game_id_single_date(home_team, away_team, game_date)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on exact date! Game ID: %s\n", game_id))
    return(game_id)
  }
  
  # If not found, try previous day (for timezone issues)
  prev_date <- as.Date(game_date) - 1
  debug_cat(sprintf("  2. Trying previous day %s (for timezone issues)...\n", prev_date))
  game_id <- find_ncaaf_game_id_single_date(home_team, away_team, prev_date)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on previous day! Game ID: %s\n", game_id))
    return(game_id)
  }
  
  # If still not found, try next day
  next_date <- as.Date(game_date) + 1
  debug_cat(sprintf("  3. Trying next day %s...\n", next_date))
  game_id <- find_ncaaf_game_id_single_date(home_team, away_team, next_date)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on next day! Game ID: %s\n", game_id))
    return(game_id)
  }
  
  # Use wider search as last resort
  debug_cat("  4. Using wider search...\n")
  return(find_ncaaf_game_id_flexible(home_team, away_team, game_date, max_days_range = 3))
}

find_ncaaf_game_id_single_date <- function(home_team, away_team, game_date) {
  tryCatch({
    games <- get_espn_ncaaf_games(game_date)
    
    if (is.null(games) || nrow(games) == 0) {
      return(NA)
    }
    
    # Clean team names for matching (NCAA-specific cleaning)
    clean_team <- function(name) {
      if (is.null(name) || is.na(name)) return("")
      name <- tolower(name)
      name <- gsub("[^a-z0-9]", "", name)
      # Handle common NCAA name variations
      name <- gsub("state", "st", name)
      name <- gsub("university", "u", name)
      name <- gsub("college", "coll", name)
      name
    }
    
    home_clean <- clean_team(home_team)
    away_clean <- clean_team(away_team)
    
    for (i in 1:nrow(games)) {
      game <- games[i, ]
      game_id <- game$id
      game_name <- tolower(game$name)
      game_short_name <- tolower(game$shortName %||% "")
      
      game_name_clean <- clean_team(game_name)
      game_short_clean <- clean_team(game_short_name)
      
      if ((grepl(home_clean, game_name_clean) && grepl(away_clean, game_name_clean)) ||
          (grepl(home_clean, game_short_clean) && grepl(away_clean, game_short_clean))) {
        debug_cat(sprintf("    Found match: %s (ID: %s)\n", game$name, game_id))
        return(game_id)
      }
    }
    
    return(NA)
    
  }, error = function(e) {
    return(NA)
  })
}

find_ncaaf_game_id_flexible <- function(home_team, away_team, game_date, max_days_range = 7) {
  base_date <- as.Date(game_date)
  
  date_offsets <- c(0)
  for (i in 1:max_days_range) {
    date_offsets <- c(date_offsets, -i, i)
  }
  
  for (offset in date_offsets) {
    current_date <- base_date + offset
    debug_cat(sprintf("    Checking %s (offset: %+d)... ", current_date, offset))
    
    game_id <- find_ncaaf_game_id_single_date(home_team, away_team, current_date)
    
    if (!is.na(game_id)) {
      debug_cat(sprintf("FOUND!\n"))
      return(game_id)
    } else {
      debug_cat("no\n")
    }
  }
  
  debug_cat("  No NCAAF game found in date range\n")
  return(NA)
}

# ==============================================
# NCAA BASKETBALL FUNCTIONS (Men's and Women's)
# ==============================================

# Get NCAAB game from ESPN API
get_espn_ncaab_games <- function(game_date) {
  tryCatch({
    espn_date <- format(as.Date(game_date), "%Y%m%d")
    url <- paste0("http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates=", espn_date)
    
    debug_cat(sprintf("  Calling ESPN NCAAB API for games on %s\n", espn_date))
    response <- GET(url, timeout = 10)
    
    if (status_code(response) != 200) {
      debug_cat(sprintf("  ESPN NCAAB API returned status: %d\n", status_code(response)))
      return(NULL)
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    if (is.null(data$events) || nrow(data$events) == 0) {
      debug_cat("  No NCAAB events found\n")
      return(NULL)
    }
    
    debug_cat(sprintf("  ESPN NCAAB API returned %d games\n", nrow(data$events)))
    return(data$events)
    
  }, error = function(e) {
    debug_cat(sprintf("  ESPN NCAAB API error: %s\n", e$message))
    return(NULL)
  })
}

# Get NCAAW game from ESPN API
get_espn_ncaaw_games <- function(game_date) {
  tryCatch({
    espn_date <- format(as.Date(game_date), "%Y%m%d")
    url <- paste0("http://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/scoreboard?dates=", espn_date)
    
    debug_cat(sprintf("  Calling ESPN NCAAW API for games on %s\n", espn_date))
    response <- GET(url, timeout = 10)
    
    if (status_code(response) != 200) {
      debug_cat(sprintf("  ESPN NCAAW API returned status: %d\n", status_code(response)))
      return(NULL)
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    if (is.null(data$events) || nrow(data$events) == 0) {
      debug_cat("  No NCAAW events found\n")
      return(NULL)
    }
    
    debug_cat(sprintf("  ESPN NCAAW API returned %d games\n", nrow(data$events)))
    return(data$events)
    
  }, error = function(e) {
    debug_cat(sprintf("  ESPN NCAAW API error: %s\n", e$message))
    return(NULL)
  })
}

# Find NCAAB game ID
find_ncaab_game_id <- function(home_team, away_team, game_date) {
  debug_cat(sprintf("\nSearching for NCAAB game: %s @ %s on %s\n", away_team, home_team, game_date))
  
  # Try exact date first
  debug_cat("  1. Trying exact date first...\n")
  game_id <- find_ncaab_game_id_single_date(home_team, away_team, game_date)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on exact date! Game ID: %s\n", game_id))
    return(game_id)
  }
  
  # If not found, try previous day
  prev_date <- as.Date(game_date) - 1
  debug_cat(sprintf("  2. Trying previous day %s...\n", prev_date))
  game_id <- find_ncaab_game_id_single_date(home_team, away_team, prev_date)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on previous day! Game ID: %s\n", game_id))
    return(game_id)
  }
  
  # If still not found, try next day
  next_date <- as.Date(game_date) + 1
  debug_cat(sprintf("  3. Trying next day %s...\n", next_date))
  game_id <- find_ncaab_game_id_single_date(home_team, away_team, next_date)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on next day! Game ID: %s\n", game_id))
    return(game_id)
  }
  
  # Use wider search as last resort
  debug_cat("  4. Using wider search...\n")
  return(find_ncaab_game_id_flexible(home_team, away_team, game_date, max_days_range = 3))
}

find_ncaab_game_id_single_date <- function(home_team, away_team, game_date) {
  tryCatch({
    games <- get_espn_ncaab_games(game_date)
    
    if (is.null(games) || nrow(games) == 0) {
      return(NA)
    }
    
    # Clean team names for matching (NCAA-specific cleaning)
    clean_team <- function(name) {
      if (is.null(name) || is.na(name)) return("")
      name <- tolower(name)
      name <- gsub("[^a-z0-9]", "", name)
      name <- gsub("state", "st", name)
      name <- gsub("university", "u", name)
      name
    }
    
    home_clean <- clean_team(home_team)
    away_clean <- clean_team(away_team)
    
    for (i in 1:nrow(games)) {
      game <- games[i, ]
      game_id <- game$id
      game_name <- tolower(game$name)
      game_short_name <- tolower(game$shortName %||% "")
      
      game_name_clean <- clean_team(game_name)
      game_short_clean <- clean_team(game_short_name)
      
      if ((grepl(home_clean, game_name_clean) && grepl(away_clean, game_name_clean)) ||
          (grepl(home_clean, game_short_clean) && grepl(away_clean, game_short_clean))) {
        debug_cat(sprintf("    Found match: %s (ID: %s)\n", game$name, game_id))
        return(game_id)
      }
    }
    
    return(NA)
    
  }, error = function(e) {
    return(NA)
  })
}

find_ncaab_game_id_flexible <- function(home_team, away_team, game_date, max_days_range = 7) {
  base_date <- as.Date(game_date)
  
  date_offsets <- c(0)
  for (i in 1:max_days_range) {
    date_offsets <- c(date_offsets, -i, i)
  }
  
  for (offset in date_offsets) {
    current_date <- base_date + offset
    debug_cat(sprintf("    Checking %s (offset: %+d)... ", current_date, offset))
    
    game_id <- find_ncaab_game_id_single_date(home_team, away_team, current_date)
    
    if (!is.na(game_id)) {
      debug_cat(sprintf("FOUND!\n"))
      return(game_id)
    } else {
      debug_cat("no\n")
    }
  }
  
  debug_cat("  No NCAAB game found in date range\n")
  return(NA)
}

# Find NCAAW game ID
find_ncaaw_game_id <- function(home_team, away_team, game_date) {
  debug_cat(sprintf("\nSearching for NCAAW game: %s @ %s on %s\n", away_team, home_team, game_date))
  
  # Try exact date first
  debug_cat("  1. Trying exact date first...\n")
  game_id <- find_ncaaw_game_id_single_date(home_team, away_team, game_date)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on exact date! Game ID: %s\n", game_id))
    return(game_id)
  }
  
  # If not found, try previous day
  prev_date <- as.Date(game_date) - 1
  debug_cat(sprintf("  2. Trying previous day %s...\n", prev_date))
  game_id <- find_ncaaw_game_id_single_date(home_team, away_team, prev_date)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on previous day! Game ID: %s\n", game_id))
    return(game_id)
  }
  
  # If still not found, try next day
  next_date <- as.Date(game_date) + 1
  debug_cat(sprintf("  3. Trying next day %s...\n", next_date))
  game_id <- find_ncaaw_game_id_single_date(home_team, away_team, next_date)
  
  if (!is.na(game_id)) {
    debug_cat(sprintf("  ✓ Found on next day! Game ID: %s\n", game_id))
    return(game_id)
  }
  
  # Use wider search as last resort
  debug_cat("  4. Using wider search...\n")
  return(find_ncaaw_game_id_flexible(home_team, away_team, game_date, max_days_range = 3))
}

find_ncaaw_game_id_single_date <- function(home_team, away_team, game_date) {
  tryCatch({
    games <- get_espn_ncaaw_games(game_date)
    
    if (is.null(games) || nrow(games) == 0) {
      return(NA)
    }
    
    # Clean team names for matching (NCAA-specific cleaning)
    clean_team <- function(name) {
      if (is.null(name) || is.na(name)) return("")
      name <- tolower(name)
      name <- gsub("[^a-z0-9]", "", name)
      name <- gsub("state", "st", name)
      name <- gsub("university", "u", name)
      name
    }
    
    home_clean <- clean_team(home_team)
    away_clean <- clean_team(away_team)
    
    for (i in 1:nrow(games)) {
      game <- games[i, ]
      game_id <- game$id
      game_name <- tolower(game$name)
      game_short_name <- tolower(game$shortName %||% "")
      
      game_name_clean <- clean_team(game_name)
      game_short_clean <- clean_team(game_short_name)
      
      if ((grepl(home_clean, game_name_clean) && grepl(away_clean, game_name_clean)) ||
          (grepl(home_clean, game_short_clean) && grepl(away_clean, game_short_clean))) {
        debug_cat(sprintf("    Found match: %s (ID: %s)\n", game$name, game_id))
        return(game_id)
      }
    }
    
    return(NA)
    
  }, error = function(e) {
    return(NA)
  })
}

find_ncaaw_game_id_flexible <- function(home_team, away_team, game_date, max_days_range = 7) {
  base_date <- as.Date(game_date)
  
  date_offsets <- c(0)
  for (i in 1:max_days_range) {
    date_offsets <- c(date_offsets, -i, i)
  }
  
  for (offset in date_offsets) {
    current_date <- base_date + offset
    debug_cat(sprintf("    Checking %s (offset: %+d)... ", current_date, offset))
    
    game_id <- find_ncaaw_game_id_single_date(home_team, away_team, current_date)
    
    if (!is.na(game_id)) {
      debug_cat(sprintf("FOUND!\n"))
      return(game_id)
    } else {
      debug_cat("no\n")
    }
  }
  
  debug_cat("  No NCAAW game found in date range\n")
  return(NA)
}

# Get NCAA basketball player stats (works for both men's and women's)
get_espn_ncaa_bball_player_stats <- function(game_id, player_name, sport) {
  tryCatch({
    # Determine the correct ESPN endpoint
    if (sport == "ncaab") {
      url <- paste0("https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event=", game_id)
    } else if (sport == "ncaaw") {
      url <- paste0("https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/summary?event=", game_id)
    } else {
      return(NULL)
    }
    
    debug_cat(sprintf("  Calling ESPN NCAA basketball API: %s\n", url))
    
    response <- GET(url, timeout = 10)
    if (status_code(response) != 200) {
      debug_cat(sprintf("  ESPN API returned status: %d\n", status_code(response)))
      return(NULL)
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    # Use the basketball stats extraction helper
    return(get_espn_basketball_player_stats_from_data(data, player_name))
    
  }, error = function(e) {
    debug_cat(sprintf("  Error getting NCAA basketball stats: %s\n", e$message))
    return(NULL)
  })
}

# Fetch NCAA basketball player stats
fetch_ncaa_bball_player_stats <- function(player_name, season, game_date = NULL, game_id = NULL, sport) {
  debug_cat(sprintf("\nDEBUG fetch_ncaa_bball_player_stats (%s):\n", sport))
  debug_cat(sprintf("  Player: %s\n", player_name))
  debug_cat(sprintf("  Season: %s\n", season))
  debug_cat(sprintf("  Game ID: %s\n", game_id))
  
  tryCatch({
    if (!is.null(game_id) && !is.na(game_id) && game_id != "NA") {
      debug_cat("  Using ESPN API for NCAA basketball player stats...\n")
      player_stats <- get_espn_ncaa_bball_player_stats(game_id, player_name, sport)
      
      if (!is.null(player_stats)) {
        return(list(
          found = TRUE,
          player = player_stats$player,
          games = 1,
          stats = list(
            points = player_stats$points,
            rebounds = player_stats$rebounds,
            assists = player_stats$assists,
            steals = player_stats$steals,
            blocks = player_stats$blocks,
            turnovers = player_stats$turnovers,
            three_pointers_made = player_stats$three_pointers_made,
            field_goals_made = player_stats$field_goals_made,
            minutes = player_stats$minutes
          )
        ))
      } else {
        return(list(found = FALSE, error = "ESPN API returned no player stats"))
      }
    }
    
    return(list(found = FALSE, error = "No game_id available"))
    
  }, error = function(e) {
    return(list(found = FALSE, error = paste("Error:", e$message)))
  })
}



# ==============================================
# IMPROVED ESPN API HELPER FUNCTIONS FOR NBA
# ==============================================

get_espn_nba_games <- function(game_date) {
  # Get NBA games from ESPN API for a specific date
  tryCatch({
    espn_date <- format(as.Date(game_date), "%Y%m%d")
    url <- paste0("http://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates=", espn_date)
    
    debug_cat(sprintf("  Calling ESPN API for games on %s\n", espn_date))
    response <- GET(url, timeout = 10)
    
    if (status_code(response) != 200) {
      debug_cat(sprintf("  ESPN API returned status: %d\n", status_code(response)))
      return(NULL)
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    if (is.null(data$events) || length(data$events) == 0) {
      debug_cat("  No events found in ESPN response\n")
      return(NULL)
    }
    
    debug_cat(sprintf("  ESPN API returned %d games\n", length(data$events)))
    return(data$events)
    
  }, error = function(e) {
    debug_cat(sprintf("ESPN API error: %s\n", e$message))
    return(NULL)
  })
}

# Alternate ESPN API endpoint
get_espn_nba_player_stats_alt <- function(game_id, player_name) {
  tryCatch({
    url <- paste0("http://cdn.espn.com/core/nba/playbyplay?gameId=", game_id, "&xhr=1")
    debug_cat(sprintf("  Trying alternate ESPN API: %s\n", url))
    
    response <- GET(url, timeout = 10)
    if (status_code(response) != 200) {
      return(NULL)
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    # Try to extract player stats from game package
    if (!is.null(data$gamepackageJSON)) {
      game_data <- fromJSON(data$gamepackageJSON)
      debug_cat("  Found gamepackageJSON in alternate API\n")
      
      # Look for player stats in game_data
      if (!is.null(game_data$boxscore) && !is.null(game_data$boxscore$players)) {
        debug_cat("  Found boxscore.players in alternate API\n")
        
        # Search for player in both teams
        for (team_type in c("homeTeam", "awayTeam")) {
          if (!is.null(game_data$boxscore$players[[team_type]])) {
            team_data <- game_data$boxscore$players[[team_type]]
            
            # Look through athletes
            if (!is.null(team_data$statistics)) {
              for (stat_group in team_data$statistics) {
                if (!is.null(stat_group$athletes)) {
                  for (athlete in stat_group$athletes) {
                    if (!is.null(athlete$athlete) && !is.null(athlete$athlete$displayName)) {
                      player_name_espn <- athlete$athlete$displayName
                      
                      # Check if player name matches
                      if (grepl(player_name, player_name_espn, ignore.case = TRUE) ||
                          grepl(player_name_espn, player_name, ignore.case = TRUE)) {
                        
                        # Extract stats
                        stats_values <- athlete$stats
                        if (is.null(stats_values) || length(stats_values) == 0) {
                          stats_values <- rep(0, 11)
                        }
                        
                        # Ensure we have at least 11 stats
                        while (length(stats_values) < 11) {
                          stats_values <- c(stats_values, 0)
                        }
                        
                        debug_cat(sprintf("    Found player via alternate API: %s\n", player_name_espn))
                        
                        return(list(
                          player = player_name_espn,
                          points = as.numeric(stats_values[1]) %||% 0,
                          rebounds = as.numeric(stats_values[2]) %||% 0,
                          assists = as.numeric(stats_values[3]) %||% 0,
                          steals = as.numeric(stats_values[4]) %||% 0,
                          blocks = as.numeric(stats_values[5]) %||% 0,
                          turnovers = as.numeric(stats_values[6]) %||% 0,
                          field_goals_made = as.numeric(stats_values[7]) %||% 0,
                          field_goals_attempted = as.numeric(stats_values[8]) %||% 0,
                          three_pointers_made = as.numeric(stats_values[9]) %||% 0,
                          free_throws_made = as.numeric(stats_values[10]) %||% 0,
                          minutes = as.numeric(stats_values[11]) %||% 0
                        ))
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    
    return(NULL)
  }, error = function(e) {
    debug_cat(sprintf("  Alternate ESPN API error: %s\n", e$message))
    return(NULL)
  })
}

get_espn_nba_player_stats <- function(game_id, player_name) {
  tryCatch({
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event=", game_id)
    debug_cat(sprintf("  Calling ESPN API for player stats: %s\n", url))
    
    response <- GET(url, timeout = 10)
    if (status_code(response) != 200) {
      debug_cat(sprintf("  ESPN API returned status: %d\n", status_code(response)))
      return(NULL)
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    # Debug: Show structure of response
    debug_cat(sprintf("  Response keys: %s\n", paste(names(data), collapse=", ")))
    
    # Check for boxscore players (CORRECT structure based on our examination)
    if (!is.null(data$boxscore) && !is.null(data$boxscore$players)) {
      debug_cat("  Found boxscore$players (correct structure)\n")
      players_df <- data$boxscore$players
      
      debug_cat(sprintf("  Found %d team(s) in boxscore\n", nrow(players_df)))
      
      # Clean name for matching
      clean_name_for_matching <- function(name) {
        name <- tolower(trimws(name))
        # Remove punctuation and common suffixes
        name <- gsub("[[:punct:]]", "", name)
        name <- gsub("\\s+[js]r$", "", name)
        name <- gsub("\\s+[ivx]+$", "", name)
        name
      }
      
      target_name_clean <- clean_name_for_matching(player_name)
      debug_cat(sprintf("  Searching for cleaned name: '%s'\n", target_name_clean))
      
      # Search through all teams
      for (team_idx in 1:nrow(players_df)) {
        team_stats <- players_df$statistics[[team_idx]]
        
        if (!is.data.frame(team_stats) || !"athletes" %in% names(team_stats)) {
          next
        }
        
        # FIXED: Handle nested list structure
        athletes_list <- team_stats$athletes
        if (!is.list(athletes_list) || length(athletes_list) == 0) {
          next
        }
        
        # The athletes might be nested in another list
        if (is.list(athletes_list[[1]])) {
          athletes_df <- athletes_list[[1]]
        } else {
          athletes_df <- athletes_list
        }
        
        if (!is.data.frame(athletes_df) || nrow(athletes_df) == 0) {
          next
        }
        
        debug_cat(sprintf("  Checking team %d with %d athletes\n", team_idx, nrow(athletes_df)))
        
        # Search for player in this team's athletes
        for (athlete_idx in 1:nrow(athletes_df)) {
          athlete_row <- athletes_df[athlete_idx, ]
          
          # FIXED: Extract player name - handle nested data frame structure
          current_player_name <- NULL
          
          # Method 1: athlete$athlete$displayName (nested data frame)
          if (!is.null(athlete_row$athlete) && is.data.frame(athlete_row$athlete)) {
            if ("displayName" %in% colnames(athlete_row$athlete)) {
              current_player_name <- athlete_row$athlete$displayName[1]
            }
          }
          
          # Method 2: athlete$displayName (direct)
          if (is.null(current_player_name) && "displayName" %in% colnames(athlete_row)) {
            current_player_name <- athlete_row$displayName
          }
          
          # Method 3: athlete$athlete.displayName (flattened)
          if (is.null(current_player_name) && "athlete.displayName" %in% colnames(athlete_row)) {
            current_player_name <- athlete_row$athlete.displayName
          }
          
          if (!is.null(current_player_name)) {
            current_name_clean <- clean_name_for_matching(current_player_name)
            
            # Check for match
            if (current_name_clean == target_name_clean ||
                grepl(target_name_clean, current_name_clean, fixed = TRUE) ||
                grepl(current_name_clean, target_name_clean, fixed = TRUE)) {
              
              debug_cat(sprintf("  FOUND MATCH: '%s' (original: '%s')\n",
                                current_name_clean, current_player_name))
              
              # Extract stats
              stats_list <- list(
                player = current_player_name,
                points = 0,
                rebounds = 0,
                assists = 0,
                steals = 0,
                blocks = 0,
                turnovers = 0,
                minutes = 0,
                field_goals_made = 0,
                field_goals_attempted = 0,
                three_pointers_made = 0,
                three_pointers_attempted = 0,
                free_throws_made = 0,
                free_throws_attempted = 0
              )
              
              # Parse stats if available
              if (!is.null(athlete_row$stats)) {
                stats_data <- athlete_row$stats
                
                # Get stats vector (it's a list with one element)
                stats_vector <- NULL
                if (is.list(stats_data) && length(stats_data) > 0) {
                  stats_vector <- stats_data[[1]]
                }
                
                if (!is.null(stats_vector) && length(stats_vector) >= 14) {
                  # ESPN stats format (from our examination)
                  # [1] minutes, [2] points, [3] FG (e.g., "6-12"), [4] 3PT, [5] FT,
                  # [6] rebounds, [7] assists, [8] steals, [9] blocks, [10] turnovers
                  
                  stats_list$minutes <- safe_numeric(stats_vector[1])
                  stats_list$points <- safe_numeric(stats_vector[2])
                  stats_list$rebounds <- safe_numeric(stats_vector[6])
                  stats_list$assists <- safe_numeric(stats_vector[7])
                  stats_list$steals <- safe_numeric(stats_vector[8])
                  stats_list$blocks <- safe_numeric(stats_vector[9])
                  stats_list$turnovers <- safe_numeric(stats_vector[10])
                  
                  # Parse shooting splits
                  if (length(stats_vector) >= 3 && grepl("-", stats_vector[3])) {
                    fg_parts <- strsplit(stats_vector[3], "-")[[1]]
                    stats_list$field_goals_made <- safe_numeric(fg_parts[1])
                    stats_list$field_goals_attempted <- safe_numeric(fg_parts[2])
                  }
                  
                  if (length(stats_vector) >= 4 && grepl("-", stats_vector[4])) {
                    three_pt_parts <- strsplit(stats_vector[4], "-")[[1]]
                    stats_list$three_pointers_made <- safe_numeric(three_pt_parts[1])
                    stats_list$three_pointers_attempted <- safe_numeric(three_pt_parts[2])
                  }
                  
                  if (length(stats_vector) >= 5 && grepl("-", stats_vector[5])) {
                    ft_parts <- strsplit(stats_vector[5], "-")[[1]]
                    stats_list$free_throws_made <- safe_numeric(ft_parts[1])
                    stats_list$free_throws_attempted <- safe_numeric(ft_parts[2])
                  }
                  
                  debug_cat(sprintf("    Stats extracted: %d pts, %d reb, %d ast, %d min\n",
                                    stats_list$points, stats_list$rebounds,
                                    stats_list$assists, stats_list$minutes))
                  
                  return(stats_list)
                } else {
                  debug_cat(sprintf("    Stats vector too short or NULL: length %s\n",
                                    if (is.null(stats_vector)) "NULL" else length(stats_vector)))
                }
              } else {
                debug_cat("    No stats found in athlete row\n")
              }
              
              # Return even with minimal info if we found the player
              return(stats_list)
            }
          }
        }
      }
      
      debug_cat(sprintf("  Player '%s' not found in any team\n", player_name))
      return(NULL)
      
    } else {
      debug_cat("  No boxscore$players found in expected structure\n")
      return(NULL)
    }
    
  }, error = function(e) {
    debug_cat(sprintf("ESPN player stats error: %s\n", e$message))
    debug_cat(sprintf("Stack trace: %s\n", paste(capture.output(traceback()), collapse = "\n")))
    return(NULL)
  })
}

# Add this helper function if not already in your code
safe_numeric <- function(x) {
  num <- suppressWarnings(as.numeric(x))
  ifelse(is.na(num), 0, num)
}

# ==============================================
# HELPER FUNCTIONS
# ==============================================
# Helper function to create search terms for team matching
create_search_terms <- function(team_name, clean_name) {
  # Basic terms - just use the cleaned name and city/team parts
  terms <- c(clean_name)
  
  # Add city name (first word) if team name has multiple words
  if (grepl("\\s", clean_name)) {
    city_part <- strsplit(clean_name, "\\s+")[[1]][1]
    terms <- c(terms, city_part)
  }
  
  # Add team name (last word) if multiple words
  if (grepl("\\s", clean_name)) {
    team_part <- strsplit(clean_name, "\\s+")[[1]]
    team_part <- team_part[length(team_part)]  # Last word
    terms <- c(terms, team_part)
  }
  
  # Remove empty terms and duplicates
  terms <- terms[nchar(terms) > 0]
  unique(terms)
}

standardize_team_name <- function(team_name, sport) {
  team_lower <- tolower(trimws(team_name))
  
  # Remove common prefixes and suffixes
  team_lower <- gsub("^(the )?\\s*", "", team_lower)
  team_lower <- gsub("\\s*\\(.*\\)$", "", team_lower)
  team_lower <- gsub("\\s*-.*$", "", team_lower)
  
  # FIX: Keep spaces! Only remove punctuation, not spaces
  # Original was: gsub("[^a-z0-9[:space:]]", "", team_lower)
  # This removes punctuation but keeps letters, numbers, AND spaces
  team_lower <- gsub("[^a-z0-9 ]", "", team_lower)  # Keep spaces
  team_lower <- gsub("\\s+", " ", team_lower)  # Normalize multiple spaces to single space
  team_lower <- trimws(team_lower)  # Trim again after cleaning
  
  debug_cat(sprintf("DEBUG standardize_team_name: Processing '%s' -> '%s' (sport: %s)\n",
                    team_name, team_lower, sport))
  
  # Create a version without spaces for matching patterns like "laclippers"
  team_lower_no_spaces <- gsub(" ", "", team_lower)
  debug_cat(sprintf("  No-spaces version: '%s'\n", team_lower_no_spaces))
  
  if (sport == "nfl") {
    mappings <- team_mappings$nfl
    patterns <- names(mappings)
    patterns <- patterns[order(-nchar(patterns))]
    
    # Try matching with spaces first
    for (pattern in patterns) {
      pattern_lower <- tolower(pattern)
      if (grepl(paste0("^", pattern_lower, "$"), team_lower) ||
          grepl(paste0("^", pattern_lower, "\\b"), team_lower) ||
          grepl(paste0("\\b", pattern_lower, "\\b"), team_lower) ||
          grepl(paste0("\\b", pattern_lower, "$"), team_lower)) {
        debug_cat(sprintf("DEBUG standardize_team_name: Matched '%s' -> '%s' using pattern '%s'\n",
                          team_name, mappings[[pattern]], pattern))
        return(mappings[[pattern]])
      }
    }
    
    # Try matching without spaces
    for (pattern in patterns) {
      pattern_lower <- tolower(pattern)
      pattern_no_spaces <- gsub(" ", "", pattern_lower)
      if (team_lower_no_spaces == pattern_no_spaces ||
          grepl(pattern_no_spaces, team_lower_no_spaces, fixed = TRUE)) {
        debug_cat(sprintf("DEBUG standardize_team_name: Matched (no spaces) '%s' -> '%s' using pattern '%s'\n",
                          team_name, mappings[[pattern]], pattern))
        return(mappings[[pattern]])
      }
    }
    
  } else if (sport == "nba") {
    mappings <- team_mappings$nba
    patterns <- names(mappings)
    patterns <- patterns[order(-nchar(patterns))]
    
    # Try matching with spaces first
    for (pattern in patterns) {
      pattern_lower <- tolower(pattern)
      if (grepl(paste0("^", pattern_lower, "$"), team_lower) ||
          grepl(paste0("^", pattern_lower, "\\b"), team_lower) ||
          grepl(paste0("\\b", pattern_lower, "\\b"), team_lower) ||
          grepl(paste0("\\b", pattern_lower, "$"), team_lower)) {
        debug_cat(sprintf("DEBUG standardize_team_name: Matched '%s' -> '%s' using pattern '%s'\n",
                          team_name, mappings[[pattern]], pattern))
        return(mappings[[pattern]])
      }
    }
    
    # Try matching without spaces (important for patterns like "laclippers")
    for (pattern in patterns) {
      pattern_lower <- tolower(pattern)
      pattern_no_spaces <- gsub(" ", "", pattern_lower)
      
      # Check if pattern matches without spaces
      if (team_lower_no_spaces == pattern_no_spaces ||
          grepl(pattern_no_spaces, team_lower_no_spaces, fixed = TRUE) ||
          grepl(team_lower_no_spaces, pattern_no_spaces, fixed = TRUE)) {
        debug_cat(sprintf("DEBUG standardize_team_name: Matched (no spaces) '%s' -> '%s' using pattern '%s'\n",
                          team_name, mappings[[pattern]], pattern))
        return(mappings[[pattern]])
      }
    }
  }
  
  # Check for abbreviation in parentheses
  paren_match <- regmatches(team_name, regexpr("\\(([A-Z]{2,4})\\)", team_name))
  if (length(paren_match) > 0) {
    abbr <- gsub("[()]", "", paren_match[1])
    debug_cat(sprintf("DEBUG standardize_team_name: Extracted abbreviation from parentheses: '%s' -> '%s'\n",
                      team_name, abbr))
    return(abbr)
  }
  
  # Fallback: check if name contains common abbreviations
  common_abbrs <- c("LAC", "LAL", "GS", "BOS", "PHX", "MIL", "DEN", "PHI", "MIA",
                    "NY", "DAL", "CLE", "SAC", "IND", "MIN", "OKC", "ORL", "NO",
                    "CHI", "ATL", "BKN", "HOU", "UTAH", "SA", "MEM", "POR", "WAS",
                    "CHA", "DET", "TOR")
  
  for (abbr in common_abbrs) {
    if (grepl(abbr, team_name, ignore.case = TRUE)) {
      debug_cat(sprintf("DEBUG standardize_team_name: Found abbreviation in name: '%s' -> '%s'\n",
                        team_name, abbr))
      return(abbr)
    }
  }
  
  # Final fallback
  result <- toupper(substr(team_name, 1, 3))
  debug_cat(sprintf("DEBUG standardize_team_name: Using fallback for '%s' -> '%s'\n", team_name, result))
  return(result)
}

extract_date_from_event <- function(event_string, provided_date = NULL) {
  debug_cat(sprintf("DEBUG extract_date_from_event: Input: '%s'\n", event_string))
  debug_cat(sprintf("DEBUG extract_date_from_event: Provided date: '%s'\n", provided_date))
  
  # First, use the provided date if available
  if (!is.null(provided_date) && provided_date != "NULL" && nchar(provided_date) > 0) {
    tryCatch({
      # Clean the provided date
      provided_date_clean <- gsub("['\"]", "", provided_date)
      # Try as.Date first (works for YYYY-MM-DD)
      result <- as.Date(provided_date_clean)
      if (!is.na(result)) {
        debug_cat(sprintf("DEBUG extract_date_from_event: Using provided date: %s\n", result))
        return(result)
      }
    }, error = function(e) {
      debug_cat(sprintf("DEBUG extract_date_from_event: Failed to parse provided date '%s': %s\n",
                        provided_date, e$message))
    })
  }
  
  # If no provided date or invalid, try to extract from event string
  patterns <- c(
    "\\d{4}-\\d{2}-\\d{2}",           # YYYY-MM-DD
    "\\d{2}/\\d{2}/\\d{4}",           # MM/DD/YYYY
    "\\d{1,2}\\.\\d{1,2}\\.\\d{4}"    # DD.MM.YYYY
  )
  
  for (pattern in patterns) {
    date_match <- regmatches(event_string, regexpr(pattern, event_string))
    if (length(date_match) > 0) {
      tryCatch({
        result <- as.Date(date_match)
        if (!is.na(result)) {
          debug_cat(sprintf("DEBUG extract_date_from_event: Found date in event string: %s\n", result))
          return(result)
        }
      }, error = function(e) {
        debug_cat(sprintf("DEBUG extract_date_from_event: Failed to parse date '%s': %s\n", date_match, e$message))
      })
    }
  }
  
  # Default to today if no date found
  result <- Sys.Date()
  debug_cat(sprintf("DEBUG extract_date_from_event: Defaulting to today: %s\n", result))
  return(result)
}

# ==============================================
# PLAYER MATCHING HELPER FUNCTION
# ==============================================

# FINAL WORKING VERSION
match_player_for_scorer <- function(player_name, scorer_name) {
  # Convert to lowercase and clean
  clean_name <- function(name) {
    name <- tolower(name)
    name <- gsub("[[:punct:]]", "", name)  # Remove all punctuation
    name <- gsub("\\s+", " ", name)        # Normalize spaces
    trimws(name)
  }
  
  player_clean <- clean_name(player_name)
  scorer_clean <- clean_name(scorer_name)
  
  # Split into words
  player_words <- strsplit(player_clean, " ")[[1]]
  scorer_words <- strsplit(scorer_clean, " ")[[1]]
  
  # Need at least one word each
  if (length(player_words) == 0 || length(scorer_words) == 0) {
    return(FALSE)
  }
  
  # Get last names
  player_last <- player_words[length(player_words)]
  scorer_last <- scorer_words[length(scorer_words)]
  
  # Get first names/initials
  player_first <- player_words[1]
  scorer_first <- scorer_words[1]
  
  # Get first letter of first name
  player_first_letter <- substr(player_first, 1, 1)
  scorer_first_letter <- substr(scorer_first, 1, 1)
  
  # DEBUG - Uncomment to see matching logic
  # cat(sprintf("\nMATCHING: '%s' vs '%s'\n", player_name, scorer_name))
  # cat(sprintf("  Cleaned: '%s' vs '%s'\n", player_clean, scorer_clean))
  # cat(sprintf("  Last names: '%s' vs '%s' (match: %s)\n",
  #             player_last, scorer_last, player_last == scorer_last))
  # cat(sprintf("  First letters: '%s' vs '%s' (match: %s)\n",
  #             player_first_letter, scorer_first_letter,
  #             player_first_letter == scorer_first_letter))
  
  # Rule 1: Exact match
  if (player_clean == scorer_clean) {
    return(TRUE)
  }
  
  # Rule 2: Same last name AND same first initial
  if (player_last == scorer_last &&
      nchar(player_first_letter) > 0 &&
      nchar(scorer_first_letter) > 0 &&
      player_first_letter == scorer_first_letter) {
    return(TRUE)
  }
  
  # Rule 3: Last name match only (more lenient)
  if (player_last == scorer_last) {
    return(TRUE)
  }
  
  # Rule 4: One contains the other
  if (grepl(player_clean, scorer_clean, fixed = TRUE) ||
      grepl(scorer_clean, player_clean, fixed = TRUE)) {
    return(TRUE)
  }
  
  return(FALSE)
}

extract_teams_from_event <- function(event_string, sport) {
  debug_cat(sprintf("\nDEBUG extract_teams_from_event: Input: '%s', Sport: '%s'\n", event_string, sport))
  
  # ==============================================
  # SPECIAL HANDLING FOR "Player First Touchdown Scorer" FORMAT (NO TEAM)
  # ==============================================
  if (grepl("Player First Touchdown Scorer", event_string, ignore.case = TRUE) &&
      !grepl("Team.*Player", event_string, ignore.case = TRUE)) {
    debug_cat("  PURE PLAYER-FIRST FORMAT: No team name in event string\n")
    debug_cat("  Format: 'PlayerName - Player First Touchdown Scorer'\n")
    return(list(
      home = "Unknown",
      away = "Unknown",
      team_only = TRUE,
      player_only = TRUE,  # New flag!
      original_format = "player_first_only",
      sport = sport
    ))
  }
  
  # ==============================================
  # SPECIAL HANDLING FOR "Team Player First Field Goal" FORMAT
  # ==============================================
  if (grepl("Team Player First Field Goal|Player First Basket|Team Player First Goal|Team Player First Goal Scorer|Team Player First", event_string, ignore.case = TRUE)) {
    
    debug_cat("  Detected special format (Team Player First X)\n")
    debug_cat(sprintf("  Sport for team lookup: %s\n", sport))
    
    # Remove the bet description part
    clean_event <- event_string
    
    # Remove everything after the last dash
    if (grepl(" - ", clean_event)) {
      # Split by dash and take the first part
      parts <- strsplit(clean_event, " - ")[[1]]
      if (length(parts) > 1) {
        clean_event <- parts[1]
        debug_cat(sprintf("  After removing bet description: '%s'\n", clean_event))
      }
    }
    
    # Clean up any remaining "Player" or "Team Player" at the end
    clean_event <- gsub("\\s+$", "", clean_event)  # Remove trailing whitespace
    clean_event <- gsub("\\s+(Team\\s+)?Player$", "", clean_event, ignore.case = TRUE)
    
    debug_cat(sprintf("  After cleaning player reference: '%s'\n", clean_event))
    
    # Now extract the team name from what's left
    words <- strsplit(clean_event, "\\s+")[[1]]
    
    if (length(words) == 0) {
      debug_cat("  ERROR: No words found after cleaning\n")
      return(NULL)
    }
    
    debug_cat(sprintf("  Words: %s\n", paste(words, collapse=", ")))
    
    # Different team endings based on sport
    if (sport %in% c("nba", "basketball")) {
      team_endings <- c(
        "76ers", "Celtics", "Lakers", "Warriors", "Bucks", "Suns", "Nuggets",
        "Heat", "Knicks", "Cavaliers", "Cavs", "Timberwolves", "Thunder",
        "Clippers", "Kings", "Grizzlies", "Pelicans", "Hawks", "Jazz",
        "Rockets", "Pacers", "Bulls", "Wizards", "Magic", "Raptors",
        "Hornets", "Pistons", "Spurs", "Trail Blazers", "Blazers", "Nets"
      )
    } else if (sport %in% c("hockey", "nhl")) {
      team_endings <- c(
        "Blackhawks", "Bruins", "Sabres", "Flames", "Hurricanes", "Avalanche",
        "Blue Jackets", "Stars", "Red Wings", "Oilers", "Panthers", "Kings",
        "Wild", "Canadiens", "Predators", "Devils", "Islanders", "Rangers",
        "Senators", "Flyers", "Coyotes", "Penguins", "Sharks", "Blues",
        "Lightning", "Maple Leafs", "Canucks", "Golden Knights", "Capitals",
        "Jets"
      )
    } else {
      # Default to both if sport not recognized
      team_endings <- c(
        "76ers", "Celtics", "Lakers", "Warriors", "Bucks", "Suns", "Nuggets",
        "Heat", "Knicks", "Cavaliers", "Timberwolves", "Thunder",
        "Clippers", "Kings", "Grizzlies", "Pelicans", "Hawks", "Jazz",
        "Rockets", "Pacers", "Bulls", "Wizards", "Magic", "Raptors",
        "Hornets", "Pistons", "Spurs", "Trail Blazers", "Nets",
        "Blackhawks", "Bruins", "Sabres", "Flames", "Hurricanes", "Avalanche",
        "Blue Jackets", "Stars", "Red Wings", "Oilers", "Panthers", "Kings",
        "Wild", "Canadiens", "Predators", "Devils", "Islanders", "Rangers",
        "Senators", "Flyers", "Coyotes", "Penguins", "Sharks", "Blues",
        "Lightning", "Maple Leafs", "Canucks", "Golden Knights", "Capitals",
        "Jets"
      )
    }
    
    # Try to find where the team name ends
    team_end_pos <- 0
    team_name_found <- NULL
    
    # Check different word combinations (1, 2, or 3 words)
    for (start_pos in 1:min(3, length(words))) {
      for (end_pos in start_pos:min(start_pos + 2, length(words))) {
        potential_team <- paste(words[start_pos:end_pos], collapse = " ")
        team_clean <- gsub("[[:punct:]]", "", potential_team)
        
        # Check if the last word is a team ending
        last_word <- words[end_pos]
        last_word_clean <- gsub("[[:punct:]]", "", last_word)
        
        if (any(sapply(team_endings, function(ending) {
          tolower(last_word_clean) == tolower(ending)
        }))) {
          team_end_pos <- end_pos
          team_name_found <- potential_team
          debug_cat(sprintf("  Found team '%s' (positions %d-%d)\n",
                            team_name_found, start_pos, end_pos))
          break
        }
      }
      if (team_end_pos > 0) break
    }
    
    # If we found a team
    if (!is.null(team_name_found)) {
      debug_cat(sprintf("  Extracted team: '%s'\n", team_name_found))
      
      return(list(
        home = team_name_found,
        away = "Unknown",
        team_only = TRUE,
        original_format = "special_first_scorer",
        sport = sport
      ))
    }
    
    # If no team ending found by pattern, try known multi-word teams
    known_multiword_teams <- c(
      "Los Angeles Lakers", "LA Lakers", "LA Clippers", "Los Angeles Clippers",
      "New York Knicks", "Golden State Warriors", "San Antonio Spurs",
      "Oklahoma City Thunder", "Portland Trail Blazers", "New Orleans Pelicans",
      "Philadelphia 76ers", "Boston Celtics", "Chicago Blackhawks",
      "New York Rangers", "Toronto Maple Leafs", "Montreal Canadiens"
    )
    
    for (known_team in known_multiword_teams) {
      if (grepl(known_team, clean_event, ignore.case = TRUE)) {
        team_name_found <- known_team
        debug_cat(sprintf("  Found known team pattern: '%s'\n", team_name_found))
        
        return(list(
          home = team_name_found,
          away = "Unknown",
          team_only = TRUE,
          original_format = "special_first_scorer",
          sport = sport
        ))
      }
    }
    
    # Final fallback: first 2-3 words
    if (length(words) >= 2) {
      team_name_found <- paste(words[1:min(3, length(words))], collapse = " ")
      debug_cat(sprintf("  Fallback: Using first words as team: '%s'\n", team_name_found))
      
      return(list(
        home = team_name_found,
        away = "Unknown",
        team_only = TRUE,
        original_format = "special_first_scorer",
        sport = sport
      ))
    }
    
    debug_cat("  Could not extract team name from special format\n")
    return(NULL)
  }
  
  # ==============================================
  # ORIGINAL LOGIC FOR NORMAL FORMATS
  # ==============================================
  
  # First, clean up the event string - remove date patterns
  event_clean <- gsub("\\s*\\(.*\\)", "", event_string)
  
  # Remove date patterns from the end
  event_clean <- gsub("\\s+\\d{4}-\\d{2}-\\d{2}$", "", event_clean)
  event_clean <- gsub("\\s+\\d{2}/\\d{2}/\\d{4}$", "", event_clean)
  event_clean <- gsub("\\s+\\d{1,2}\\.\\d{1,2}\\.\\d{4}$", "", event_clean)
  
  debug_cat(sprintf("DEBUG extract_teams_from_event: Cleaned: '%s'\n", event_clean))
  
  # Common separators in order of preference
  separators <- c(" @ ", " vs ", " at ", " v ", " vs. ", " - ")
  
  for (sep in separators) {
    if (grepl(sep, event_clean, fixed = TRUE)) {
      parts <- strsplit(event_clean, sep, fixed = TRUE)[[1]]
      if (length(parts) >= 2) {
        away_team <- trimws(parts[1])
        home_team <- trimws(parts[2])
        
        # Remove any remaining date or extraneous info
        home_team <- gsub("\\s+\\d{4}.*$", "", home_team)
        home_team <- gsub("\\s+on\\s+.*$", "", home_team, ignore.case = TRUE)
        
        debug_cat(sprintf("DEBUG extract_teams_from_event: Using separator '%s'\n", sep))
        debug_cat(sprintf("  away: '%s'\n", away_team))
        debug_cat(sprintf("  home: '%s'\n", home_team))
        
        return(list(home = home_team, away = away_team, team_only = FALSE))
      }
    }
  }
  
  # If no separator found, try regex pattern
  pattern <- "([A-Za-z.\\s']+?)\\s+(?:@|vs|at|vs\\.|v|-)\\s+([A-Za-z.\\s']+)"
  match <- regmatches(event_clean, regexpr(pattern, event_clean, perl = TRUE))
  if (length(match) > 0) {
    away_match <- regmatches(match, regexec(pattern, match))[[1]][2]
    home_match <- regmatches(match, regexec(pattern, match))[[1]][3]
    
    if (!is.na(away_match) && !is.na(home_match)) {
      debug_cat(sprintf("DEBUG extract_teams_from_event: Using regex pattern\n"))
      debug_cat(sprintf("  away: '%s'\n", trimws(away_match)))
      debug_cat(sprintf("  home: '%s'\n", trimws(home_match)))
      return(list(home = trimws(home_match), away = trimws(away_match), team_only = FALSE))
    }
  }
  
  # Last resort: split on any non-alphabetic character that might be a separator
  separators_regex <- "[@vVsS-]+"
  if (grepl(separators_regex, event_clean)) {
    parts <- strsplit(event_clean, separators_regex)[[1]]
    if (length(parts) >= 2) {
      away_team <- trimws(parts[1])
      home_team <- trimws(parts[2])
      debug_cat(sprintf("DEBUG extract_teams_from_event: Using regex separator fallback\n"))
      debug_cat(sprintf("  away: '%s'\n", away_team))
      debug_cat(sprintf("  home: '%s'\n", home_team))
      return(list(home = home_team, away = away_team, team_only = FALSE))
    }
  }
  
  debug_cat(sprintf("WARNING: Could not extract teams from: '%s'\n", event_string))
  debug_cat(sprintf("  Cleaned version: '%s'\n", event_clean))
  return(NULL)
}

# ==============================================
# FIND GAME FOR TEAM-ONLY CASES (FIRST SCORER FORMATS)
# ==============================================

find_nba_game_for_team_only <- function(team_name, game_date, days_range = 14) {
  debug_cat(sprintf("\n🔍 FINDING GAME FOR TEAM-ONLY: '%s' around %s (±%d days)\n",
                    team_name, game_date, days_range))
  
  base_date <- as.Date(game_date)
  
  # Clean team name for matching
  clean_team_name <- function(name) {
    if (is.null(name) || is.na(name) || name == "") return("")
    name <- tolower(name)
    name <- gsub("[^a-z0-9]", "", name)
    name
  }
  
  team_clean <- clean_team_name(team_name)
  debug_cat(sprintf("  Looking for team pattern: '%s' (cleaned: '%s')\n", team_name, team_clean))
  
  if (team_clean == "") {
    debug_cat("  ERROR: Empty team name after cleaning\n")
    return(NULL)
  }
  
  # Try dates in order: exact, -1, +1, -2, +2, etc.
  date_offsets <- c(0)
  for (i in 1:days_range) {
    date_offsets <- c(date_offsets, -i, i)
  }
  
  all_matches <- list()
  
  for (offset in date_offsets) {
    current_date <- base_date + offset
    debug_cat(sprintf("  Checking date %s (offset: %+d)... ", current_date, offset))
    
    # Get all games on this date using ESPN API
    games <- tryCatch({
      get_all_nba_games_on_date(current_date)
    }, error = function(e) {
      debug_cat(sprintf("error getting games: %s\n", e$message))
      list()
    })
    
    if (length(games) > 0) {
      debug_cat(sprintf("found %d games\n", length(games)))
      
      # Check each game for our team
      for (game in games) {
        # Extract teams from matchup string (format: "Away Team @ Home Team")
        if (grepl(" @ ", game$matchup)) {
          parts <- strsplit(game$matchup, " @ ")[[1]]
          away_team_found <- parts[1]
          home_team_found <- parts[2]
          
          # Clean found teams
          home_clean <- clean_team_name(home_team_found)
          away_clean <- clean_team_name(away_team_found)
          
          # CRITICAL FIX: Check if our team matches EITHER home OR away
          team_match <- FALSE
          match_position <- ""
          actual_team_found <- ""
          
          # Check if our team is contained in either team name
          if (grepl(team_clean, home_clean) || grepl(home_clean, team_clean)) {
            team_match <- TRUE
            match_position <- "home"
            actual_team_found <- home_team_found
          }
          
          if (!team_match && (grepl(team_clean, away_clean) || grepl(away_clean, team_clean))) {
            team_match <- TRUE
            match_position <- "away"
            actual_team_found <- away_team_found
          }
          
          if (team_match) {
            debug_cat(sprintf("    ✓ Found match! %s (team is %s)\n",
                              game$matchup, match_position))
            
            # Check game status
            game_status <- tryCatch({
              check_nba_game_status(game$game_id)
            }, error = function(e) {
              list(exists = TRUE, completed = FALSE)  # Assume exists if we can't check
            })
            
            # Store match with priority
            priority_score <- 100 - abs(offset) * 5  # Higher score for closer dates
            
            # Bonus for completed games
            if (is.list(game_status) && !is.null(game_status$completed) && game_status$completed) {
              priority_score <- priority_score + 20
            }
            
            all_matches[[length(all_matches) + 1]] <- list(
              game_id = game$game_id,
              home_team = home_team_found,
              away_team = away_team_found,
              actual_date = as.character(current_date),
              offset = offset,
              priority = priority_score,
              matchup = game$matchup,
              score = game$score,
              status = if (!is.null(game$status)) game$status else "Unknown",
              completed = if (is.list(game_status) && !is.null(game_status$completed)) game_status$completed else FALSE,
              our_team_position = match_position,
              our_team_found = actual_team_found
            )
          }
        }
      }
    } else {
      debug_cat("no games\n")
    }
    
    # Small delay to avoid rate limiting
    if (abs(offset) %% 3 == 0) {
      Sys.sleep(0.05)
    }
  }
  
  if (length(all_matches) > 0) {
    # Sort matches by priority (closest date first, completed games preferred)
    all_matches <- all_matches[order(sapply(all_matches, function(x) x$priority), decreasing = TRUE)]
    
    best_match <- all_matches[[1]]
    debug_cat(sprintf("\n  Selected best match: %s on %s (offset: %+d days)\n",
                      best_match$matchup, best_match$actual_date, best_match$offset))
    debug_cat(sprintf("    Game ID: %s, Score: %s, Status: %s\n",
                      best_match$game_id, best_match$score, best_match$status))
    debug_cat(sprintf("    Our team '%s' found as %s team\n",
                      team_name, best_match$our_team_position))
    
    # Verify the game exists and get complete info
    game_status <- check_nba_game_status(best_match$game_id)
    if (game_status$exists) {
      debug_cat(sprintf("    Verified: %s @ %s\n", game_status$away_team, game_status$home_team))
      debug_cat(sprintf("    Game status: %s (completed: %s)\n",
                        game_status$status, game_status$completed))
    }
    
    return(list(
      game_id = best_match$game_id,
      home_team = best_match$home_team,
      away_team = best_match$away_team,
      actual_date = best_match$actual_date,
      matchup = best_match$matchup,
      score = best_match$score,
      status = best_match$status,
      completed = best_match$completed,
      our_team_position = best_match$our_team_position,
      our_team_found = best_match$our_team_found
    ))
  }
  
  debug_cat(sprintf("\n  ✗ No games found for team '%s' in %d-day range\n", team_name, days_range * 2))
  debug_cat(sprintf("  Team cleaned as: '%s'\n", team_clean))
  
  return(NULL)
}

# ==============================================
# UPDATED FIND GAME ID FUNCTION WITH ALL SPORTS
# ==============================================

find_game_id <- function(home_team, away_team, game_date, sport, team_only_mode = FALSE) {
  debug_cat(sprintf("\nDEBUG find_game_id:\n"))
  debug_cat(sprintf("  Date: %s\n", game_date))
  debug_cat(sprintf("  Home team input: '%s'\n", home_team))
  debug_cat(sprintf("  Away team input: '%s'\n", away_team))
  debug_cat(sprintf("  Sport: %s\n", sport))
  debug_cat(sprintf("  Team-only mode: %s\n", team_only_mode))
  
  # ========== CRITICAL: CHECK IF GAME IS IN THE FUTURE ==========
  current_date <- Sys.Date()
  game_date_obj <- as.Date(game_date)
  
  if (game_date_obj > current_date) {
    debug_cat(sprintf("  ⚠️ FUTURE GAME: %s is in the future (today: %s)\n",
                      game_date_obj, current_date))
    
    # For NBA, try to find future games in ESPN schedule
    if (sport %in% c("nba", "basketball")) {
      debug_cat("  Looking for future NBA game in ESPN schedule...\n")
      
      # Use ESPN API to find scheduled games
      tryCatch({
        result <- find_nba_game_id_timezone_simple(game_date_obj, away_team, home_team)
        if (!is.null(result) && !is.na(result$game_id)) {
          debug_cat(sprintf("  ✓ Found future game in ESPN schedule: %s\n", result$game_id))
          return(result$game_id)
        }
      }, error = function(e) {
        debug_cat(sprintf("  Error finding future game: %s\n", e$message))
      })
    }
    
    # Return a special game_id for future games
    future_id <- paste0("FUTURE_", format(game_date_obj, "%Y%m%d"), "_",
                        substr(tolower(home_team), 1, 3), "_",
                        substr(tolower(away_team), 1, 3))
    debug_cat(sprintf("  Using placeholder game ID for future game: %s\n", future_id))
    return(future_id)
  }
  
  # Check if we're dealing with a "team_only" case (from special format)
  team_only_case <- FALSE
  if (is.list(home_team) && !is.null(home_team$team_only) && home_team$team_only == TRUE) {
    debug_cat("  SPECIAL CASE: Team-only mode (from first scorer format)\n")
    team_info <- home_team
    home_team <- team_info$home
    away_team <- team_info$away
    team_sport <- if (!is.null(team_info$sport)) team_info$sport else sport
    team_only_case <- TRUE
    debug_cat(sprintf("    Extracted: home='%s', away='%s', sport='%s'\n",
                      home_team, away_team, team_sport))
    
    # Update sport if different from team info
    if (!is.null(team_sport) && team_sport != "") {
      sport <- team_sport
    }
  }
  
  # If team_only_mode parameter is TRUE, override
  if (team_only_mode) {
    team_only_case <- TRUE
    debug_cat("  Team-only mode parameter is TRUE\n")
  }
  
  # Normalize sport name
  sport <- normalize_sport_name(sport)
  debug_cat(sprintf("Normalized sport: %s\n", sport))
  
  # Backup check for NCAA games (in case they get past resolve_bet)
  if (sport == "nfl" || sport == "football") {
    # Quick NCAA check using team names
    ncaa_quick_check <- function(team_name) {
      team_lower <- tolower(team_name)
      ncaa_indicators <- c("state$", "tech$", "university", "college", "oregon", "texas tech")
      any(sapply(ncaa_indicators, function(indicator) grepl(indicator, team_lower)))
    }
    
    if (ncaa_quick_check(home_team) || ncaa_quick_check(away_team)) {
      debug_cat("ERROR: College team detected in find_game_id\n")
      return(NA)
    }
  }
  
  tryCatch({
    if (sport == "nfl") {
      # Use intelligent season detection for NFL
      season_year <- detect_correct_season("nfl", game_date)
      debug_cat(sprintf("  Using NFL season: %s (for game date %s)\n", season_year, game_date))
      
      # Check data availability with fallback
      season_to_use <- check_and_fix_season_availability(sport, season_year, game_date)
      if (season_to_use != season_year) {
        debug_cat(sprintf("  ⚠️ Using fallback season: %s (original: %s)\n", season_to_use, season_year))
      }
      
      debug_cat(sprintf("  Loading NFL schedule for season: %s\n", season_to_use))
      schedules <- nflreadr::load_schedules(season_to_use)
      
      if (nrow(schedules) == 0) {
        debug_cat("ERROR: No schedule data loaded\n")
        return(NA)
      }
      
      debug_cat(sprintf("  Loaded %d games in schedule\n", nrow(schedules)))
      
      home_std <- standardize_team_name(home_team, sport)
      away_std <- standardize_team_name(away_team, sport)
      
      home_clean <- nflreadr::clean_team_abbrs(home_std)
      away_clean <- nflreadr::clean_team_abbrs(away_std)
      
      debug_cat(sprintf("  Home: '%s' -> '%s' -> '%s'\n", home_team, home_std, home_clean))
      debug_cat(sprintf("  Away: '%s' -> '%s' -> '%s'\n", away_team, away_std, away_clean))
      
      if (is.na(home_clean) || is.na(away_clean)) {
        debug_cat("ERROR: Could not clean team abbreviations\n")
        return(NA)
      }
      
      debug_cat(sprintf("  Looking for games on date: %s\n", game_date))
      games_on_date <- schedules %>% filter(as.Date(gameday) == as.Date(game_date))
      debug_cat(sprintf("  Found %d games on that date\n", nrow(games_on_date)))
      
      if (nrow(games_on_date) > 0) {
        debug_cat("  Games on that date:\n")
        for (i in 1:nrow(games_on_date)) {
          g <- games_on_date[i, ]
          debug_cat(sprintf("    Game %d: %s @ %s (gameday: %s)\n",
                            i, g$away_team, g$home_team, g$gameday))
        }
        
        game <- games_on_date %>%
          filter(
            (nflreadr::clean_team_abbrs(home_team) == home_clean &
               nflreadr::clean_team_abbrs(away_team) == away_clean) |
              (nflreadr::clean_team_abbrs(home_team) == away_clean &
                 nflreadr::clean_team_abbrs(away_team) == home_clean)
          )
        
        if (nrow(game) > 0) {
          debug_cat(sprintf("SUCCESS: Found matching game! Game ID: %s\n", game$game_id[1]))
          debug_cat(sprintf("  Match details: %s @ %s on %s\n",
                            game$away_team[1], game$home_team[1], game$gameday[1]))
          return(game$game_id[1])
        } else {
          debug_cat("  No exact match found. Checking nearby dates...\n")
          
          # Wider date range search for NFL too
          date_range <- seq(as.Date(game_date) - 7, as.Date(game_date) + 7, by = "days")
          nearby_games <- schedules %>%
            filter(as.Date(gameday) %in% date_range) %>%
            filter(
              (nflreadr::clean_team_abbrs(home_team) == home_clean &
                 nflreadr::clean_team_abbrs(away_team) == away_clean) |
                (nflreadr::clean_team_abbrs(home_team) == away_clean &
                   nflreadr::clean_team_abbrs(away_team) == home_clean)
            )
          
          if (nrow(nearby_games) > 0) {
            debug_cat(sprintf("  Found nearby game! Game ID: %s\n", nearby_games$game_id[1]))
            debug_cat(sprintf("    Match details: %s @ %s on %s (expected: %s)\n",
                              nearby_games$away_team[1], nearby_games$home_team[1],
                              nearby_games$gameday[1], game_date))
            return(nearby_games$game_id[1])
          }
        }
      } else {
        debug_cat("  No games on exact date. Searching entire season for team match...\n")
        all_season_games <- schedules %>%
          filter(
            (nflreadr::clean_team_abbrs(home_team) == home_clean &
               nflreadr::clean_team_abbrs(away_team) == away_clean) |
              (nflreadr::clean_team_abbrs(home_team) == away_clean &
                 nflreadr::clean_team_abbrs(away_team) == home_clean)
          )
        
        if (nrow(all_season_games) > 0) {
          debug_cat(sprintf("  Found game in season! Game ID: %s\n", all_season_games$game_id[1]))
          debug_cat(sprintf("    Match details: %s @ %s on %s (expected: %s)\n",
                            all_season_games$away_team[1], all_season_games$home_team[1],
                            all_season_games$gameday[1], game_date))
          return(all_season_games$game_id[1])
        }
      }
      
      debug_cat("ERROR: No matching game found in schedule\n")
      return(NA)
      
    } else if (sport == "nba") {
      game_date_obj <- as.Date(game_date)
      
      # SPECIAL HANDLING FOR TEAM-ONLY CASES (from special formats or team_only_mode)
      if (team_only_case || away_team == "Unknown" || away_team == "Unknown Opponent" ||
          is.null(away_team) || away_team == "") {
        debug_cat(sprintf("  SPECIAL: Looking for any game with team '%s' on or near %s\n",
                          home_team, game_date))
        
        # Determine which team we're looking for
        target_team <- home_team
        if (home_team == "Unknown" || home_team == "Unknown Opponent" || home_team == "") {
          target_team <- away_team
        }
        
        # Use a different search approach when we only have one team
        result <- find_nba_game_for_team_only(target_team, game_date)
        
        if (!is.null(result) && !is.na(result$game_id)) {
          debug_cat(sprintf("  ✓ Found game for team '%s': %s @ %s (ID: %s)\n",
                            target_team, result$away_team, result$home_team, result$game_id))
          return(result$game_id)
        } else {
          debug_cat(sprintf("  ✗ Could not find any game for team '%s' around that date\n", target_team))
          
          # Try one more time with a simpler approach
          debug_cat("  Trying simple verification as last resort...\n")
          simple_id <- find_nba_game_id_simple_verification(game_date_obj, "Unknown", target_team)
          if (!is.na(simple_id)) {
            debug_cat(sprintf("  ✓ Found via simple verification: %s\n", simple_id))
            return(simple_id)
          }
          
          return(NA)
        }
      }
      
      debug_cat(sprintf("  Looking for NBA game: %s @ %s on %s\n",
                        away_team, home_team, game_date))
      
      # Use timezone-aware search for NBA games
      debug_cat("  Using timezone-aware search for NBA game...\n")
      
      # Use the simple timezone-aware function
      result <- find_nba_game_id_timezone_simple(game_date, away_team, home_team)
      
      if (!is.null(result)) {
        debug_cat(sprintf("  ✓ Game found via timezone-aware search!\n"))
        debug_cat(sprintf("    Game ID: %s\n", result$game_id))
        
        if (result$days_off != 0) {
          debug_cat(sprintf("    ⚠️ Date discrepancy: Expected %s, found %s (%+d days)\n",
                            result$searched_date %||% game_date, result$actual_date, result$days_off))
          debug_cat(sprintf("    Note: %s\n", result$note))
          
          if (abs(result$days_off) == 1) {
            debug_cat(sprintf("    This is likely a timezone issue with late-night games\n"))
          }
        }
        
        return(result$game_id)
      }
      
      debug_cat("  No game found with timezone-aware search, trying fallback...\n")
      
      # Fallback to original flexible search
      flexible_result <- find_nba_game_id_flexible(
        game_date = game_date,
        away_team_search = away_team,
        home_team_search = home_team,
        max_days_range = 14
      )
      
      if (!is.null(flexible_result)) {
        debug_cat(sprintf("  ✓ Game found via fallback search!\n"))
        return(flexible_result$game_id)
      }
      
      debug_cat("  No game found in date range. Trying fallback methods...\n")
      
      # Fallback 1: Try single date with fixed ESPN API function
      game_id <- find_nba_game_id_single_date(game_date, away_team, home_team)
      if (!is.na(game_id)) {
        debug_cat(sprintf("  Found NBA game ID via ESPN API: %s\n", game_id))
        return(game_id)
      }
      
      # Fallback 2: Try hoopR
      debug_cat("  Trying hoopR fallback...\n")
      tryCatch({
        # Use intelligent season detection for NBA
        season_year <- detect_correct_season("nba", game_date)
        debug_cat(sprintf("  Using NBA season: %s (for game date %s)\n", season_year, game_date))
        
        schedule <- hoopR::load_nba_schedule(season = season_year)
        
        if (nrow(schedule) > 0) {
          schedule$game_date <- as.Date(schedule$game_date)
          
          home_clean <- gsub("[^a-z0-9]", "", tolower(home_team))
          away_clean <- gsub("[^a-z0-9]", "", tolower(away_team))
          
          game <- schedule %>%
            filter(game_date == game_date_obj) %>%
            mutate(
              home_name_clean = gsub("[^a-z0-9]", "", tolower(home_team_name)),
              away_name_clean = gsub("[^a-z0-9]", "", tolower(away_team_name))
            ) %>%
            filter(
              (grepl(home_clean, home_name_clean) & grepl(away_clean, away_name_clean)) |
                (grepl(home_clean, away_name_clean) & grepl(away_clean, home_name_clean))
            )
          
          if (nrow(game) > 0 && !is.null(game$game_id)) {
            debug_cat(sprintf("  Found NBA game ID via hoopR: %s\n", game$game_id[1]))
            return(game$game_id[1])
          }
        }
      }, error = function(e) {
        debug_cat(sprintf("  hoopR error: %s\n", e$message))
      })
      
      # Last resort: Show all games on the estimated date for debugging
      debug_cat("  No game found. Showing all games on estimated date for debugging:\n")
      games_on_date <- get_all_nba_games_on_date(game_date)
      if (length(games_on_date) > 0) {
        for (i in seq_along(games_on_date)) {
          game_info <- games_on_date[[i]]
          debug_cat(sprintf("    Game %d: %s (ID: %s) Score: %s Status: %s\n",
                            i, game_info$matchup, game_info$game_id,
                            game_info$score, game_info$status))
        }
      } else {
        debug_cat("    No games found on estimated date\n")
      }
      
      debug_cat("  All search methods failed, trying direct verification...\n")
      
      # Try simple verification first
      verified_id <- find_nba_game_id_simple_verification(game_date_obj, away_team, home_team)
      if (!is.na(verified_id)) {
        debug_cat(sprintf("  ✓ Found via simple verification: %s\n", verified_id))
        return(verified_id)
      }
      
      # If simple fails, try comprehensive verification
      debug_cat("  Simple verification failed, trying comprehensive verification...\n")
      verified_id <- find_nba_game_id_by_verification(game_date_obj, away_team, home_team)
      if (!is.na(verified_id)) {
        debug_cat(sprintf("  ✓ Found via comprehensive verification: %s\n", verified_id))
        return(verified_id)
      }
      
      debug_cat("ERROR: Could not find game ID\n")
      return(NA)
      
    } else if (sport == "nhl") {
      # Use ESPN API for NHL games with robust finder
      debug_cat(sprintf("  Looking for NHL game: %s @ %s on %s\n",
                        away_team, home_team, game_date))
      
      game_date_obj <- as.Date(game_date)
      
      # Use the robust NHL game finder function
      nhl_game_id <- find_nhl_game_id_robust(home_team, away_team, game_date)
      
      if (!is.na(nhl_game_id)) {
        debug_cat(sprintf("  ✓ Found NHL game ID: %s\n", nhl_game_id))
        return(nhl_game_id)
      }
      
      debug_cat("ERROR: Could not find NHL game ID\n")
      return(NA)
      
    } else if (sport == "mlb") {
      # MLB
      debug_cat(sprintf("  Looking for MLB game: %s @ %s on %s\n",
                        away_team, home_team, game_date))
      mlb_game_id <- find_mlb_game_id(home_team, away_team, game_date)
      if (!is.na(mlb_game_id)) {
        debug_cat(sprintf("  ✓ Found MLB game ID: %s\n", mlb_game_id))
        return(mlb_game_id)
      }
      debug_cat("ERROR: Could not find MLB game ID\n")
      return(NA)
      
    } else if (sport == "wnba") {
      # WNBA
      debug_cat(sprintf("  Looking for WNBA game: %s @ %s on %s\n",
                        away_team, home_team, game_date))
      wnba_game_id <- find_wnba_game_id(home_team, away_team, game_date)
      if (!is.na(wnba_game_id)) {
        debug_cat(sprintf("  ✓ Found WNBA game ID: %s\n", wnba_game_id))
        return(wnba_game_id)
      }
      debug_cat("ERROR: Could not find WNBA game ID\n")
      return(NA)
      
    } else if (sport == "ncaaf") {
      # NCAA Football
      debug_cat(sprintf("  Looking for NCAAF game: %s @ %s on %s\n",
                        away_team, home_team, game_date))
      ncaaf_game_id <- find_ncaaf_game_id(home_team, away_team, game_date)
      if (!is.na(ncaaf_game_id)) {
        debug_cat(sprintf("  ✓ Found NCAAF game ID: %s\n", ncaaf_game_id))
        return(ncaaf_game_id)
      }
      debug_cat("ERROR: Could not find NCAAF game ID\n")
      return(NA)
      
    } else if (sport == "ncaab") {
      # NCAA Men's Basketball
      debug_cat(sprintf("  Looking for NCAAB game: %s @ %s on %s\n",
                        away_team, home_team, game_date))
      ncaab_game_id <- find_ncaab_game_id(home_team, away_team, game_date)
      if (!is.na(ncaab_game_id)) {
        debug_cat(sprintf("  ✓ Found NCAAB game ID: %s\n", ncaab_game_id))
        return(ncaab_game_id)
      }
      debug_cat("ERROR: Could not find NCAAB game ID\n")
      return(NA)
      
    } else if (sport == "ncaaw") {
      # NCAA Women's Basketball
      debug_cat(sprintf("  Looking for NCAAW game: %s @ %s on %s\n",
                        away_team, home_team, game_date))
      ncaaw_game_id <- find_ncaaw_game_id(home_team, away_team, game_date)
      if (!is.na(ncaaw_game_id)) {
        debug_cat(sprintf("  ✓ Found NCAAW game ID: %s\n", ncaaw_game_id))
        return(ncaaw_game_id)
      }
      debug_cat("ERROR: Could not find NCAAW game ID\n")
      return(NA)
      
    } else if (sport == "hockey") {
      # Legacy handling - redirect to NHL
      debug_cat("  Redirecting 'hockey' to 'nhl' handling...\n")
      return(find_game_id(home_team, away_team, game_date, "nhl", team_only_mode))
      
    } else {
      debug_cat(sprintf("  Sport '%s' is not supported\n", sport))
      return(NA)
    }
    
  }, error = function(e) {
    debug_cat(sprintf("ERROR in find_game_id: %s\n", e$message))
    debug_cat(sprintf("  Stack trace:\n"))
    debug_print(traceback())
    return(NA)
  })
}

# ==============================================
# FIXED NBA PLAYER STATS WITH ESPN API (FOR 2025-26 SEASON)
# ==============================================

fetch_nba_player_stats <- function(player_name, season, game_date = NULL, game_id = NULL) {
  debug_cat(sprintf("\nDEBUG fetch_nba_player_stats:\n"))
  debug_cat(sprintf("  Player: %s\n", player_name))
  debug_cat(sprintf("  Season: %s\n", season))
  debug_cat(sprintf("  Game Date: %s\n", game_date))
  debug_cat(sprintf("  Game ID: %s\n", game_id))
  
  tryCatch({
    # FORCE ESPN API FOR NBA WHEN game_id IS AVAILABLE - NO HOOPR FALLBACK FOR 2025-26
    if (!is.null(game_id) && !is.na(game_id) && game_id != "NA") {
      debug_cat("  Using ESPN API for player stats (forced for 2025-26 season)...\n")
      player_stats <- get_espn_nba_player_stats(game_id, player_name)
      
      if (!is.null(player_stats)) {
        debug_cat(sprintf("  Found player stats via ESPN API\n"))
        debug_cat(sprintf("    Points: %s, Rebounds: %s, Assists: %s\n",
                          player_stats$points, player_stats$rebounds, player_stats$assists))
        
        return(list(
          found = TRUE,
          player = player_stats$player,
          games = 1,
          stats = list(
            points = player_stats$points,
            rebounds = player_stats$rebounds,
            assists = player_stats$assists,
            steals = player_stats$steals,
            blocks = player_stats$blocks,
            turnovers = player_stats$turnovers,
            field_goals_made = player_stats$field_goals_made,
            field_goals_attempted = player_stats$field_goals_attempted,
            three_pointers_made = player_stats$three_pointers_made,
            free_throws_made = player_stats$free_throws_made,
            minutes = player_stats$minutes
          )
        ))
      } else {
        # ESPN API failed - provide better error
        debug_cat("  ESPN API returned NULL player stats\n")
        return(list(found = FALSE, error = "ESPN API returned no player stats for this game"))
      }
    }
    
    # If we get here, we don't have a game_id or ESPN failed
    debug_cat("ERROR: No game_id provided for ESPN API lookup\n")
    return(list(found = FALSE, error = "No game_id available for ESPN API"))
    
  }, error = function(e) {
    debug_cat(sprintf("ERROR in fetch_nba_player_stats: %s\n", e$message))
    return(list(found = FALSE, error = paste("Error fetching NBA stats:", e$message)))
  })
}

# ==============================================
# NFL PLAYER STATS WITH SEASON DETECTION FIX
# ==============================================

fetch_nfl_player_stats <- function(player_name, season, week = NULL, game_id = NULL) {
  debug_cat(sprintf("\nDEBUG fetch_nfl_player_stats:\n"))
  debug_cat(sprintf("  Player: %s\n", player_name))
  debug_cat(sprintf("  Season: %s\n", season))
  debug_cat(sprintf("  Week: %s\n", week))
  debug_cat(sprintf("  Game ID: %s\n", game_id))
  
  tryCatch({
    # Check data availability first
    debug_cat("  Checking data availability...\n")
    season_to_use <- check_and_fix_season_availability("nfl", season, Sys.Date())
    
    if (season_to_use != season) {
      debug_cat(sprintf("  ⚠️ Using adjusted season: %s (was: %s)\n", season_to_use, season))
      season <- season_to_use
    }
    
    debug_cat("  Loading player stats...\n")
    stats_df <- nflreadr::load_player_stats(seasons = season)
    
    if (nrow(stats_df) == 0) {
      debug_cat("ERROR: No player stats loaded\n")
      return(list(found = FALSE, error = "No player stats available"))
    }
    
    debug_cat(sprintf("  Loaded stats for %d player-game combinations\n", nrow(stats_df)))
    
    # Remove NA rows
    stats_df <- stats_df[!is.na(stats_df$player_display_name), ]
    
    player_col <- "player_display_name"
    debug_cat(sprintf("  Using column '%s' for player matching\n", player_col))
    
    # Helper function to clean player names
    clean_player_name_for_search <- function(player_name) {
      cleaned <- gsub("\\s+Sr\\.?$", "", player_name, ignore.case = TRUE)
      cleaned <- gsub("\\s+Jr\\.?$", "", cleaned, ignore.case = TRUE)
      cleaned <- gsub("\\s+III$", "", cleaned, ignore.case = TRUE)
      cleaned <- gsub("\\s+II$", "", cleaned, ignore.case = TRUE)
      cleaned <- gsub("\\s+I$", "", cleaned, ignore.case = TRUE)
      cleaned <- gsub("\\s+V$", "", cleaned, ignore.case = TRUE)
      return(trimws(cleaned))
    }
    
    # Clean player name for search
    player_cleaned <- clean_player_name_for_search(player_name)
    debug_cat(sprintf("  Looking for cleaned name: '%s'\n", player_cleaned))
    
    # Try multiple matching strategies
    player_stats <- stats_df[stats_df[[player_col]] == player_cleaned, ]
    
    if (nrow(player_stats) == 0) {
      # Try original name
      debug_cat("  No match with cleaned name, trying original name...\n")
      player_stats <- stats_df[stats_df[[player_col]] == player_name, ]
    }
    
    if (nrow(player_stats) == 0) {
      # Try without any suffixes
      debug_cat("  No match with original name, trying without suffixes...\n")
      without_suffix <- gsub("\\s+[JS]r\\.?$", "", player_name, ignore.case = TRUE)
      without_suffix <- gsub("\\s+[IVX]+$", "", without_suffix, ignore.case = TRUE)
      without_suffix <- trimws(without_suffix)
      player_stats <- stats_df[stats_df[[player_col]] == without_suffix, ]
    }
    
    if (nrow(player_stats) == 0) {
      # Try case-insensitive
      debug_cat("  No match without suffixes, trying case-insensitive...\n")
      player_stats <- stats_df[tolower(stats_df[[player_col]]) == tolower(player_cleaned), ]
    }
    
    if (nrow(player_stats) == 0) {
      # Try partial match
      debug_cat("  No case-insensitive match, trying partial match...\n")
      player_stats <- stats_df[grepl(player_cleaned, stats_df[[player_col]], ignore.case = TRUE), ]
    }
    
    debug_cat(sprintf("  Found %d matching rows after name cleaning\n", nrow(player_stats)))
    
    if (nrow(player_stats) == 0) {
      debug_cat(sprintf("ERROR: Player '%s' not found in stats\n", player_name))
      return(list(found = FALSE, error = paste("Player", player_name, "not found")))
    }
    
    debug_cat(sprintf("  Found %d games for player\n", nrow(player_stats)))
    
    # Filter by week if provided
    if (!is.null(week)) {
      player_stats <- player_stats[player_stats$week == week, ]
      debug_cat(sprintf("  After filtering by week %s: %d games\n", week, nrow(player_stats)))
    }
    
    # Filter by game_id if provided
    if (!is.null(game_id)) {
      debug_cat("  NOTE: game_id filtering not available in player stats. Using schedule to find week...\n")
      
      # Load schedule to get week from game_id
      schedule <- nflreadr::load_schedules(seasons = season)
      game_info <- schedule[schedule$game_id == game_id, ]
      
      if (nrow(game_info) > 0) {
        game_week <- game_info$week[1]
        debug_cat(sprintf("  Game ID %s corresponds to week %s\n", game_id, game_week))
        player_stats <- player_stats[player_stats$week == game_week, ]
        debug_cat(sprintf("  After filtering by week from game_id: %d games\n", nrow(player_stats)))
      } else {
        debug_cat(sprintf("  WARNING: Could not find game_id %s in schedule\n", game_id))
      }
    }
    
    if (nrow(player_stats) == 0) {
      debug_cat("ERROR: No games match the criteria\n")
      return(list(found = FALSE, error = "No games match the criteria"))
    }
    
    # Show what we found
    debug_cat("  Games found:\n")
    for (i in 1:min(3, nrow(player_stats))) {
      ps <- player_stats[i, ]
      
      player_name_found <- if(!is.null(ps[[player_col]])) ps[[player_col]] else "Unknown"
      week_val <- if(!is.null(ps$week)) ps$week else "NA"
      team_val <- if(!is.null(ps$team)) ps$team else "NA"
      opponent_val <- if(!is.null(ps$opponent_team)) ps$opponent_team else "NA"
      
      yards <- sum(
        if(!is.null(ps$passing_yards)) ps$passing_yards else 0,
        if(!is.null(ps$rushing_yards)) ps$rushing_yards else 0,
        if(!is.null(ps$receiving_yards)) ps$receiving_yards else 0,
        na.rm = TRUE
      )
      
      tds <- sum(
        if(!is.null(ps$passing_tds)) ps$passing_tds else 0,
        if(!is.null(ps$rushing_tds)) ps$rushing_tds else 0,
        if(!is.null(ps$receiving_tds)) ps$receiving_tds else 0,
        na.rm = TRUE
      )
      
      receptions <- if(!is.null(ps$receptions)) ps$receptions else 0
      
      debug_cat(sprintf("    Game %d: %s (Week %s), %s vs %s, %s yards, %s TDs, %s receptions\n",
                        i, player_name_found, week_val, team_val, opponent_val, yards, tds, receptions))
    }
    
    # Build result
    result <- list(
      found = TRUE,
      player = if(!is.null(player_stats[[player_col]][1])) player_stats[[player_col]][1] else player_name,
      games = nrow(player_stats),
      stats = list(
        passing_yards = sum(player_stats$passing_yards, na.rm = TRUE),
        passing_tds = sum(player_stats$passing_tds, na.rm = TRUE),
        passing_ints = sum(player_stats$passing_interceptions, na.rm = TRUE),
        rushing_yards = sum(player_stats$rushing_yards, na.rm = TRUE),
        rushing_tds = sum(player_stats$rushing_tds, na.rm = TRUE),
        receiving_yards = sum(player_stats$receiving_yards, na.rm = TRUE),
        receiving_tds = sum(player_stats$receiving_tds, na.rm = TRUE),
        receptions = sum(player_stats$receptions, na.rm = TRUE),
        targets = sum(player_stats$targets, na.rm = TRUE),
        fantasy_points = sum(player_stats$fantasy_points, na.rm = TRUE),
        completions = sum(player_stats$completions, na.rm = TRUE),
        attempts = sum(player_stats$attempts, na.rm = TRUE)
      )
    )
    
    # Add game details if we have exactly one game
    if (nrow(player_stats) == 1) {
      game_details <- list()
      if (!is.null(player_stats$week[1])) game_details$week = player_stats$week[1]
      if (!is.null(player_stats$opponent_team[1])) game_details$opponent = player_stats$opponent_team[1]
      if (!is.null(player_stats$team[1])) game_details$team = player_stats$team[1]
      if (length(game_details) > 0) result$game_details = game_details
    }
    
    result$stats$total_tds <- result$stats$passing_tds + result$stats$rushing_tds + result$stats$receiving_tds
    
    debug_cat(sprintf("  Final stats: %s yards, %s TDs, %s receptions\n",
                      result$stats$rushing_yards + result$stats$receiving_yards + result$stats$passing_yards,
                      result$stats$total_tds,
                      result$stats$receptions))
    
    return(result)
    
  }, error = function(e) {
    debug_cat(sprintf("ERROR in fetch_nfl_player_stats: %s\n", e$message))
    return(list(found = FALSE, error = paste("Error fetching NFL stats:", e$message)))
  })
}

# ==============================================
# UPDATED FIRST SCORER FUNCTION WITH ESPN API FALLBACK
# ==============================================
fetch_first_scorer <- function(game_id, sport, season) {
  debug_cat(sprintf("\nDEBUG fetch_first_scorer:\n"))
  debug_cat(sprintf("  Game ID: %s\n", game_id))
  debug_cat(sprintf("  Sport: %s\n", sport))
  debug_cat(sprintf("  Season: %s\n", season))
  
  tryCatch({
    if (sport == "nfl") {
      debug_cat("  Loading NFL play-by-play data...\n")
      pbp <- nflreadr::load_pbp(season)
      
      if (nrow(pbp) == 0) {
        debug_cat("ERROR: No play-by-play data loaded\n")
        return(list(found = FALSE, error = "No play-by-play data available"))
      }
      
      game_plays <- pbp[pbp$game_id == game_id, ]
      debug_cat(sprintf("  Found %d plays for game\n", nrow(game_plays)))
      
      if (nrow(game_plays) == 0) {
        debug_cat("ERROR: Game not found in play-by-play data\n")
        return(list(found = FALSE, error = "Game not found in play-by-play data"))
      }
      
      scoring_plays <- game_plays %>%
        filter(!is.na(td_team) | (!is.na(field_goal_result) & field_goal_result == "made")) %>%
        arrange(play_id)
      
      debug_cat(sprintf("  Found %d scoring plays\n", nrow(scoring_plays)))
      
      if (nrow(scoring_plays) > 0) {
        first_play <- scoring_plays[1, ]
        
        result <- list(
          found = TRUE,
          play_type = ifelse(!is.na(first_play$td_team), "touchdown", "field goal"),
          scorer = ifelse(!is.na(first_play$td_team),
                          first_play$td_player_name,
                          first_play$kicker_player_name),
          team = ifelse(!is.na(first_play$td_team),
                        first_play$td_team,
                        first_play$posteam),
          quarter = first_play$qtr,
          time = first_play$game_seconds_remaining,
          description = first_play$desc
        )
        
        debug_cat(sprintf("  First scorer: %s (%s)\n", result$scorer, result$play_type))
        return(result)
      } else {
        debug_cat("ERROR: No scoring plays found\n")
        return(list(found = FALSE, error = "No scoring plays found"))
      }
      
    } else if (sport == "nba") {
      # CRITICAL FIX: Try ESPN API first for 2025-26 season
      debug_cat("  Checking if we need ESPN API (season 2025 or later)...\n")
      
      if (season >= 2025) {
        debug_cat(sprintf("  Season %s >= 2025, using ESPN API\n", season))
        espn_result <- fetch_first_scorer_espn_nba(game_id, season)
        
        if (espn_result$found) {
          debug_cat(sprintf("  ESPN API success: %s\n", espn_result$scorer))
          return(espn_result)
        } else {
          debug_cat(sprintf("  ESPN API failed: %s\n", espn_result$error))
        }
      }
      # Fallback to hoopR for older seasons
      debug_cat("  Falling back to hoopR...\n")
      game_pbp <- tryCatch({
        hoopR::load_nba_pbp(season = season)
      }, error = function(e) {
        debug_cat(sprintf("  ERROR loading NBA PBP: %s\n", e$message))
        NULL
      })
      
      if (is.null(game_pbp) || nrow(game_pbp) == 0) {
        debug_cat("  Could not load NBA play-by-play\n")
        return(list(found = FALSE, error = "Could not load play-by-play data"))
      }
      
      debug_cat(sprintf("  Loaded %d NBA plays\n", nrow(game_pbp)))
      
      # Look for scoring plays
      if ("scoringPlay" %in% colnames(game_pbp)) {
        scoring_plays <- game_pbp %>%
          filter(scoringPlay == TRUE) %>%
          arrange(id)
      } else {
        debug_cat("  Could not identify scoring plays\n")
        return(list(found = FALSE, error = "Could not identify scoring plays"))
      }
      
      debug_cat(sprintf("  Found %d scoring plays\n", nrow(scoring_plays)))
      
      if (nrow(scoring_plays) > 0) {
        first_score <- scoring_plays[1, ]
        
        scorer <- "Unknown"
        scorer_cols <- c("athlete_id_1", "text", "description", "playText")
        for (col in scorer_cols) {
          if (col %in% colnames(first_score) && !is.na(first_score[[col]][1])) {
            scorer <- first_score[[col]][1]
            break
          }
        }
        
        return(list(
          found = TRUE,
          scorer = scorer,
          team = if ("homeAway" %in% colnames(first_score)) first_score$homeAway[1] else "Unknown",
          points = if ("scoreValue" %in% colnames(first_score)) first_score$scoreValue[1] else 0,
          quarter = if ("period" %in% colnames(first_score)) first_score$period$number[1] else 1,
          time = if ("clock" %in% colnames(first_score)) first_score$clock$displayValue[1] else "Unknown",
          source = "hoopR"
        ))
      }
    }
    
    debug_cat("ERROR: No scoring plays found\n")
    return(list(found = FALSE, error = "No scoring plays found"))
    
  }, error = function(e) {
    debug_cat(sprintf("ERROR in fetch_first_scorer: %s\n", e$message))
    return(list(found = FALSE, error = paste("Error fetching first scorer:", e$message)))
  })
}

# ==============================================
# UPDATED LAST SCORER FUNCTION WITH ESPN API FALLBACK
# ==============================================

fetch_last_scorer <- function(game_id, sport, season) {
  debug_cat(sprintf("\nDEBUG fetch_last_scorer:\n"))
  debug_cat(sprintf("  Game ID: %s\n", game_id))
  debug_cat(sprintf("  Sport: %s\n", sport))
  debug_cat(sprintf("  Season: %s\n", season))
  
  tryCatch({
    if (sport == "nfl") {
      pbp <- nflreadr::load_pbp(season)
      game_plays <- pbp[pbp$game_id == game_id, ]
      
      if (nrow(game_plays) == 0) {
        debug_cat("ERROR: Game not found\n")
        return(list(found = FALSE, error = "Game not found"))
      }
      
      scoring_plays <- game_plays %>%
        filter(!is.na(td_team) | (!is.na(field_goal_result) & field_goal_result == "made")) %>%
        arrange(desc(play_id))
      
      debug_cat(sprintf("  Found %d scoring plays (looking for last)\n", nrow(scoring_plays)))
      
      if (nrow(scoring_plays) > 0) {
        last_play <- scoring_plays[1, ]
        
        result <- list(
          found = TRUE,
          play_type = ifelse(!is.na(last_play$td_team), "touchdown", "field goal"),
          scorer = ifelse(!is.na(last_play$td_team),
                          last_play$td_player_name,
                          last_play$kicker_player_name),
          team = ifelse(!is.na(last_play$td_team),
                        last_play$td_team,
                        last_play$posteam),
          quarter = last_play$qtr,
          time = last_play$game_seconds_remaining,
          description = last_play$desc
        )
        
        debug_cat(sprintf("  Last scorer: %s (%s)\n", result$scorer, result$play_type))
        return(result)
      }
      
    } else if (sport == "nba") {
      # CRITICAL FIX: Try ESPN API first for 2025-26 season
      debug_cat("  Checking if we need ESPN API (season 2025 or later)...\n")
      
      if (season >= 2025) {
        debug_cat(sprintf("  Season %s >= 2025, using ESPN API\n", season))
        espn_result <- fetch_last_scorer_espn_nba(game_id, season)
        
        if (espn_result$found) {
          debug_cat(sprintf("  ESPN API success: %s\n", espn_result$scorer))
          return(espn_result)
        } else {
          debug_cat(sprintf("  ESPN API failed: %s\n", espn_result$error))
        }
      }
      # Fallback to hoopR for older seasons
      debug_cat("  Falling back to hoopR...\n")
      game_pbp <- tryCatch({
        hoopR::load_nba_pbp(season = season)
      }, error = function(e) {
        debug_cat(sprintf("  ERROR loading NBA PBP: %s\n", e$message))
        NULL
      })
      
      if (!is.null(game_pbp) && nrow(game_pbp) > 0) {
        if ("scoringPlay" %in% colnames(game_pbp)) {
          scoring_plays <- game_pbp %>%
            filter(scoringPlay == TRUE) %>%
            arrange(desc(id))
        } else {
          return(list(found = FALSE, error = "Could not identify scoring plays"))
        }
        
        if (nrow(scoring_plays) > 0) {
          last_score <- scoring_plays[1, ]
          
          scorer <- "Unknown"
          scorer_cols <- c("athlete_id_1", "text", "description", "playText")
          for (col in scorer_cols) {
            if (col %in% colnames(last_score) && !is.na(last_score[[col]][1])) {
              scorer <- last_score[[col]][1]
              break
            }
          }
          
          return(list(
            found = TRUE,
            scorer = scorer,
            team = if ("homeAway" %in% colnames(last_score)) last_score$homeAway[1] else "Unknown",
            points = if ("scoreValue" %in% colnames(last_score)) last_score$scoreValue[1] else 0,
            quarter = if ("period" %in% colnames(last_score)) last_score$period$number[1] else 1,
            time = if ("clock" %in% colnames(last_score)) last_score$clock$displayValue[1] else "Unknown",
            source = "hoopR"
          ))
        }
      }
    }
    
    debug_cat("ERROR: No scoring plays found\n")
    return(list(found = FALSE, error = "No scoring plays found"))
    
  }, error = function(e) {
    debug_cat(sprintf("ERROR in fetch_last_scorer: %s\n", e$message))
    return(list(found = FALSE, error = paste("Error fetching last scorer:", e$message)))
  })
}

fetch_game_result <- function(game_id, sport, season) {
  debug_cat("\nDEBUG fetch_game_result:\n")
  debug_cat(sprintf("  Game ID: %s\n", game_id))
  debug_cat(sprintf("  Sport: %s\n", sport))
  debug_cat(sprintf("  Season: %s\n", season))
  
  result <- list(
    found = FALSE,
    home_score = NA,
    away_score = NA,
    total_points = NA,
    home_team = NA,
    away_team = NA,
    error = NULL
  )
  
  tryCatch({
    if (sport %in% c("nba", "basketball")) {
      # Use the SUMMARY endpoint which has structured data
      debug_cat("  Using ESPN Summary API for NBA...\n")
      summary_url <- sprintf("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event=%s", game_id)
      
      summary_response <- tryCatch({
        fromJSON(summary_url)
      }, error = function(e) {
        debug_cat(sprintf("  Summary API request failed: %s\n", e$message))
        return(NULL)
      })
      
      if (!is.null(summary_response) && is.list(summary_response)) {
        # Method 1: Extract from header competitions
        if (!is.null(summary_response$header) &&
            !is.null(summary_response$header$competitions)) {
          
          competitions <- summary_response$header$competitions
          
          if (is.data.frame(competitions) && "competitors" %in% names(competitions) &&
              length(competitions$competitors) > 0) {
            
            competitors_df <- competitions$competitors[[1]]
            
            if (is.data.frame(competitors_df) && nrow(competitors_df) >= 2) {
              # Find home and away teams
              home_rows <- competitors_df[competitors_df$homeAway == "home", ]
              away_rows <- competitors_df[competitors_df$homeAway == "away", ]
              
              if (nrow(home_rows) > 0 && nrow(away_rows) > 0) {
                home_row <- home_rows[1, ]
                away_row <- away_rows[1, ]
                
                result$home_score <- as.numeric(home_row$score %||% 0)
                result$away_score <- as.numeric(away_row$score %||% 0)
                result$home_team <- home_row$team$displayName
                result$away_team <- away_row$team$displayName
                result$total_points <- result$home_score + result$away_score
                result$found <- TRUE
                
                debug_cat(sprintf("  Summary API success: %s %s - %s %s\n",
                                  result$away_team, result$away_score,
                                  result$home_team, result$home_score))
                debug_cat(sprintf("  Total points: %s\n", result$total_points))
                return(result)
              }
            }
          }
        }
        
        # Method 2: Try boxscore as fallback
        if (!result$found && !is.null(summary_response$boxscore)) {
          debug_cat("  Trying boxscore data...\n")
        }
      }
      
      # If still not found, provide error
      if (!result$found) {
        result$error <- "Game not found in ESPN data"
      }
      
    } else if (sport %in% c("nfl", "football")) {
      # Use nflreadr for NFL game result
      debug_cat("  Using nflreadr for NFL game result...\n")
      
      # Load NFL schedule for the season
      schedule <- nflreadr::load_schedules(seasons = season)
      
      if (nrow(schedule) == 0) {
        debug_cat("  No schedule data available for season\n")
        result$error <- "No schedule data available"
        return(result)
      }
      
      # Find the specific game
      game_row <- schedule[schedule$game_id == game_id, ]
      
      if (nrow(game_row) == 0) {
        debug_cat(sprintf("  Game ID %s not found in schedule\n", game_id))
        result$error <- "Game not found in schedule"
        return(result)
      }
      
      # Extract game information
      result$home_team <- game_row$home_team[1]
      result$away_team <- game_row$away_team[1]
      result$home_score <- as.numeric(game_row$home_score[1])
      result$away_score <- as.numeric(game_row$away_score[1])
      
      # Check if game has been played
      if (is.na(result$home_score) || is.na(result$away_score)) {
        debug_cat("  Game not completed yet\n")
        result$error <- "Game not completed yet"
        result$found <- FALSE
        return(result)
      }
      
      result$total_points <- result$home_score + result$away_score
      result$found <- TRUE
      
      debug_cat(sprintf("  NFL game result: %s %d - %d %s\n",
                        result$away_team, result$away_score,
                        result$home_score, result$home_team))
      
    } else if (sport %in% c("mlb", "baseball")) {
      # MLB support using ESPN API
      debug_cat("  Using ESPN API for MLB game result...\n")
      
      url <- paste0("https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary?event=", game_id)
      debug_cat(sprintf("  URL: %s\n", url))
      response <- GET(url, timeout = 10)
      
      if (status_code(response) != 200) {
        debug_cat(sprintf("  ESPN MLB API returned status: %d\n", status_code(response)))
        result$error <- paste("ESPN API returned status", status_code(response))
        return(result)
      }
      
      data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
      
      # Extract game result from header competitions
      if (!is.null(data$header) && !is.null(data$header$competitions)) {
        competitions <- data$header$competitions
        
        if (is.data.frame(competitions) && nrow(competitions) > 0) {
          comp <- competitions[1, ]
          
          # Get teams and scores
          if (!is.null(comp$competitors) && length(comp$competitors) > 0) {
            competitors_df <- comp$competitors[[1]]
            
            if (is.data.frame(competitors_df) && nrow(competitors_df) >= 2) {
              for (i in 1:nrow(competitors_df)) {
                team_info <- competitors_df[i, ]
                
                if (!is.null(team_info$homeAway)) {
                  if (team_info$homeAway == "home") {
                    result$home_team <- team_info$team.displayName %||% team_info$team$displayName
                    result$home_score <- as.numeric(team_info$score %||% 0)
                    debug_cat(sprintf("  Home team: %s, Score: %d\n", result$home_team, result$home_score))
                  } else if (team_info$homeAway == "away") {
                    result$away_team <- team_info$team.displayName %||% team_info$team$displayName
                    result$away_score <- as.numeric(team_info$score %||% 0)
                    debug_cat(sprintf("  Away team: %s, Score: %d\n", result$away_team, result$away_score))
                  }
                }
              }
              
              # Check if game is completed
              game_completed <- FALSE
              if (!is.null(comp$status) && !is.null(comp$status$type$completed)) {
                game_completed <- as.logical(comp$status$type$completed)
                debug_cat(sprintf("  Game completed status from API: %s\n", game_completed))
              }
              
              # Also check if we have non-zero scores (indicates game was played)
              if (game_completed || (result$home_score > 0 || result$away_score > 0)) {
                result$found <- TRUE
                result$total_points <- result$home_score + result$away_score
                result$total_runs <- result$total_points  # MLB uses runs
                
                debug_cat(sprintf("  MLB game result: %s %d - %d %s\n",
                                  result$away_team, result$away_score,
                                  result$home_score, result$home_team))
                debug_cat(sprintf("  Total runs: %d\n", result$total_points))
                return(result)
              } else {
                result$error <- "Game not completed yet (no scores available)"
                debug_cat(sprintf("  %s\n", result$error))
              }
            } else {
              result$error <- "Could not parse competitors data"
              debug_cat(sprintf("  %s\n", result$error))
            }
          } else {
            result$error <- "No competitors data found"
            debug_cat(sprintf("  %s\n", result$error))
          }
        } else {
          result$error <- "No competitions data found"
          debug_cat(sprintf("  %s\n", result$error))
        }
      } else {
        result$error <- "Could not extract MLB game result from ESPN data"
        debug_cat(sprintf("  %s\n", result$error))
      }
      
      if (!result$found && is.null(result$error)) {
        result$error <- "MLB game result extraction failed"
      }
      
    } else if (sport %in% c("nhl", "hockey")) {
      # NHL support using ESPN API
      debug_cat("  Using ESPN API for NHL game result...\n")
      
      url <- paste0("https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary?event=", game_id)
      response <- GET(url, timeout = 10)
      
      if (status_code(response) == 200) {
        data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
        
        if (!is.null(data$boxscore) && !is.null(data$boxscore$teams)) {
          teams_data <- data$boxscore$teams
          
          if (is.data.frame(teams_data)) {
            for (i in 1:nrow(teams_data)) {
              team_info <- teams_data[i, ]
              if (!is.null(team_info$homeAway)) {
                if (team_info$homeAway == "home") {
                  result$home_team <- team_info$team$displayName %||% team_info$team.displayName
                  result$home_score <- as.numeric(team_info$score %||% 0)
                } else if (team_info$homeAway == "away") {
                  result$away_team <- team_info$team$displayName %||% team_info$team.displayName
                  result$away_score <- as.numeric(team_info$score %||% 0)
                }
              }
            }
            
            if (!is.na(result$home_score) && !is.na(result$away_score)) {
              result$found <- TRUE
              result$total_points <- result$home_score + result$away_score
              debug_cat(sprintf("  NHL game result: %s %d - %d %s\n",
                                result$away_team, result$away_score,
                                result$home_score, result$home_team))
            }
          }
        }
      }
      
      if (!result$found && is.null(result$error)) {
        result$error <- "Could not extract NHL game result"
      }
      
    } else {
      result$error <- paste("Sport not supported:", sport)
      debug_cat(sprintf("  %s\n", result$error))
    }
    
  }, error = function(e) {
    debug_cat(sprintf("  Unexpected error in fetch_game_result: %s\n", e$message))
    result$error <- paste("Unexpected error:", e$message)
  })
  
  if (!result$found && is.null(result$error)) {
    result$error <- "Game not found"
  }
  
  debug_cat(sprintf("  Final result: found=%s\n", result$found))
  if (!is.null(result$error)) {
    debug_cat(sprintf("  Error: %s\n", result$error))
  }
  
  return(result)
}

# ==============================================
# PERIOD/INNING DATA EXTRACTION FUNCTIONS
# ==============================================

# Extract inning number from period list
extract_inning_number <- function(period) {
  if(is.list(period) && !is.null(period$number)) {
    return(period$number)
  }
  if(is.numeric(period)) {
    return(period)
  }
  return(NA)
}

# Get inning-by-inning runs from play-by-play
get_inning_runs <- function(plays, away_team, home_team) {
  if(is.null(plays)) return(NULL)
  
  # Get scoring plays
  scoring_plays <- plays[!is.na(plays$scoringPlay) & plays$scoringPlay == TRUE, ]
  if(nrow(scoring_plays) == 0) return(NULL)
  
  # Extract inning numbers
  scoring_plays$inning <- sapply(scoring_plays$period, extract_inning_number)
  
  # Calculate runs per inning
  inning_runs <- data.frame()
  for(inn in 1:9) {
    inn_scoring <- scoring_plays[scoring_plays$inning == inn, ]
    away_inn <- 0
    home_inn <- 0
    
    if(nrow(inn_scoring) > 0) {
      for(i in 1:nrow(inn_scoring)) {
        text <- tolower(inn_scoring$text[i])
        if(grepl(tolower(away_team), text) || grepl(tolower(gsub(" .*", "", away_team)), text)) {
          away_inn <- away_inn + inn_scoring$scoreValue[i]
        } else {
          home_inn <- home_inn + inn_scoring$scoreValue[i]
        }
      }
    }
    
    inning_runs <- rbind(inning_runs, data.frame(
      inning = inn,
      away_runs = away_inn,
      home_runs = home_inn,
      total_runs = away_inn + home_inn
    ))
  }
  
  return(inning_runs)
}

# Get cumulative scores throughout the game
get_cumulative_scores <- function(plays, away_team, home_team) {
  if(is.null(plays)) return(NULL)
  
  scoring_plays <- plays[!is.na(plays$scoringPlay) & plays$scoringPlay == TRUE, ]
  if(nrow(scoring_plays) == 0) return(NULL)
  
  scoring_plays$inning <- sapply(scoring_plays$period, extract_inning_number)
  
  cumulative <- data.frame()
  cum_away <- 0
  cum_home <- 0
  
  for(i in 1:nrow(scoring_plays)) {
    text <- tolower(scoring_plays$text[i])
    if(grepl(tolower(away_team), text) || grepl(tolower(gsub(" .*", "", away_team)), text)) {
      cum_away <- cum_away + scoring_plays$scoreValue[i]
    } else {
      cum_home <- cum_home + scoring_plays$scoreValue[i]
    }
    
    cumulative <- rbind(cumulative, data.frame(
      play_index = i,
      inning = scoring_plays$inning[i],
      away_score = cum_away,
      home_score = cum_home,
      deficit = abs(cum_away - cum_home),
      leader = if(cum_away > cum_home) away_team else if(cum_home > cum_away) home_team else "Tie"
    ))
  }
  
  return(cumulative)
}

# Get NBA quarter scores
get_nba_quarter_scores <- function(game_data) {
  if(is.null(game_data$boxscore$periodScores)) return(NULL)
  
  ps <- game_data$boxscore$periodScores
  result <- list()
  
  for(i in 1:nrow(ps)) {
    result[[i]] <- list(
      quarter = i,
      away_score = as.numeric(ps$awayScore[i]),
      home_score = as.numeric(ps$homeScore[i]),
      total = as.numeric(ps$awayScore[i]) + as.numeric(ps$homeScore[i])
    )
  }
  
  return(result)
}

# FIXED: Get NBA quarter scores from the correct location
get_nba_quarter_scores <- function(game_data) {
  # NBA quarter scores are in header$competitions$competitors$linescores
  if(is.null(game_data$header$competitions)) return(NULL)
  
  comp <- game_data$header$competitions
  if(is.null(comp$competitors) || length(comp$competitors) == 0) return(NULL)
  
  competitors <- comp$competitors[[1]]
  
  away_scores <- c()
  home_scores <- c()
  
  for(i in 1:nrow(competitors)) {
    competitor <- competitors[i, ]
    linescores <- competitor$linescores
    
    if(!is.null(linescores) && length(linescores) > 0) {
      linescores_df <- linescores[[1]]
      
      if(competitor$homeAway == "home") {
        home_scores <- as.numeric(linescores_df$value)
      } else {
        away_scores <- as.numeric(linescores_df$value)
      }
    }
  }
  
  if(length(home_scores) == 0 || length(away_scores) == 0) return(NULL)
  
  result <- list()
  for(i in 1:length(home_scores)) {
    result[[i]] <- list(
      quarter = i,
      away_score = away_scores[i],
      home_score = home_scores[i],
      total = away_scores[i] + home_scores[i]
    )
  }
  
  return(result)
}

# For NHL, the period scores are in boxscore$periods (your existing function is correct)
get_nhl_period_scores <- function(game_data) {
  if(is.null(game_data$boxscore$periods)) return(NULL)
  
  periods <- game_data$boxscore$periods
  result <- list()
  
  for(i in 1:nrow(periods)) {
    result[[i]] <- list(
      period = i,
      away_score = as.numeric(periods$awayScore[i]),
      home_score = as.numeric(periods$homeScore[i]),
      total = as.numeric(periods$awayScore[i]) + as.numeric(periods$homeScore[i])
    )
  }
  
  return(result)
}

# ==============================================
# MARKET-SPECIFIC RESOLUTION FUNCTIONS
# ==============================================

# Resolve inning total runs
resolve_inning_total_runs <- function(game_data, inning_num, line_value, direction) {
  inning_runs <- get_inning_runs(game_data$plays, "", "")
  if(is.null(inning_runs) || inning_num > nrow(inning_runs)) {
    return(list(success = FALSE, error = "Inning data not available"))
  }
  
  total <- inning_runs$total_runs[inning_num]
  line <- as.numeric(line_value)
  
  result <- list(
    success = TRUE,
    actual_value = total,
    line_value = line
  )
  
  if(direction == "over") {
    result$bet_won <- total > line
  } else if(direction == "under") {
    result$bet_won <- total < line
  }
  
  return(result)
}

# Resolve multi-inning total runs

# Function to resolve multi-inning total runs - CORRECT VERSION using linescores
resolve_multi_inning_total_runs <- function(game_data, num_innings, line_value, direction) {
  result <- list(success = FALSE, actual_value = 0, bet_won = NULL, error = NULL)
  
  tryCatch({
    if (is.null(game_data) || is.null(game_data$header$competitions)) {
      result$error <- "No game data available"
      return(result)
    }
    
    total_runs <- 0
    
    # Extract inning runs from header$competitions$competitors$linescores
    comp <- game_data$header$competitions
    if (is.data.frame(comp) && nrow(comp) > 0) {
      if ("competitors" %in% names(comp) && length(comp$competitors) > 0) {
        competitors <- comp$competitors[[1]]
        
        if (is.data.frame(competitors) && nrow(competitors) >= 2) {
          away_linescores <- NULL
          home_linescores <- NULL
          
          # Find home and away teams' linescores
          for (i in 1:nrow(competitors)) {
            if (competitors$homeAway[i] == "away") {
              if ("linescores" %in% names(competitors) && length(competitors$linescores) >= i) {
                away_linescores <- competitors$linescores[[i]]
                debug_cat(sprintf("  Found away team linescores with %d innings\n", nrow(away_linescores)))
              }
            } else if (competitors$homeAway[i] == "home") {
              if ("linescores" %in% names(competitors) && length(competitors$linescores) >= i) {
                home_linescores <- competitors$linescores[[i]]
                debug_cat(sprintf("  Found home team linescores with %d innings\n", nrow(home_linescores)))
              }
            }
          }
          
          # Calculate runs for specified number of innings
          if (!is.null(away_linescores) && !is.null(home_linescores)) {
            max_innings <- max(nrow(away_linescores), nrow(home_linescores))
            
            for (inning in 1:min(num_innings, max_innings)) {
              away_runs <- 0
              home_runs <- 0
              
              if (inning <= nrow(away_linescores)) {
                away_runs <- as.numeric(away_linescores$displayValue[inning])
                if (is.na(away_runs)) away_runs <- 0
              }
              if (inning <= nrow(home_linescores)) {
                home_runs <- as.numeric(home_linescores$displayValue[inning])
                if (is.na(home_runs)) home_runs <- 0
              }
              
              inning_total <- away_runs + home_runs
              total_runs <- total_runs + inning_total
              
              debug_cat(sprintf("  Inning %d: Away=%d, Home=%d, Inning Total=%d, Cumulative=%d\n", 
                                inning, away_runs, home_runs, inning_total, total_runs))
            }
            
            result$success <- TRUE
            result$actual_value <- total_runs
            
            if (!is.null(line_value) && !is.null(direction)) {
              line_num <- as.numeric(line_value)
              if (direction == "over") {
                result$bet_won <- total_runs > line_num
              } else if (direction == "under") {
                result$bet_won <- total_runs < line_num
              }
              debug_cat(sprintf("  Final: %d runs, Line: %.1f, %s -> Bet won: %s\n", 
                                total_runs, line_num, direction, 
                                if(!is.null(result$bet_won)) result$bet_won else "NULL"))
            }
          } else {
            result$error <- "Could not find linescores data for both teams"
            debug_cat("  ERROR: Could not find linescores data for both teams\n")
          }
        } else {
          result$error <- "Could not find competitors data"
        }
      } else {
        result$error <- "No competitors data in game header"
      }
    } else {
      result$error <- "No competition data in game header"
    }
    
  }, error = function(e) {
    result$error <- paste("Error in resolve_multi_inning_total_runs:", e$message)
    debug_cat(sprintf("  ERROR: %s\n", e$message))
  })
  
  return(result)
}

# Resolve inning moneyline
resolve_inning_moneyline <- function(game_data, inning_num, bet_team = NULL) {
  inning_runs <- get_inning_runs(game_data$plays, "", "")
  if(is.null(inning_runs) || inning_num > nrow(inning_runs)) {
    return(list(success = FALSE, error = "Inning data not available"))
  }
  
  away_runs <- inning_runs$away_runs[inning_num]
  home_runs <- inning_runs$home_runs[inning_num]
  
  result <- list(
    success = TRUE,
    away_score = away_runs,
    home_score = home_runs
  )
  
  if(away_runs > home_runs) {
    result$winner <- "away"
    result$bet_won <- (!is.null(bet_team) && bet_team == "away")
  } else if(home_runs > away_runs) {
    result$winner <- "home"
    result$bet_won <- (!is.null(bet_team) && bet_team == "home")
  } else {
    result$winner <- "tie"
    result$bet_won <- FALSE
  }
  
  return(result)
}

# Resolve largest comeback
resolve_largest_comeback <- function(game_data) {
  cumulative <- get_cumulative_scores(game_data$plays, "", "")
  if(is.null(cumulative)) {
    return(list(success = FALSE, error = "Play-by-play data not available"))
  }
  
  max_deficit <- max(cumulative$deficit, na.rm = TRUE)
  
  return(list(
    success = TRUE,
    largest_comeback = max_deficit
  ))
}

# Resolve team score first and win
resolve_team_score_first_and_win <- function(game_data, team_name) {
  plays <- game_data$plays
  scoring_plays <- plays[!is.na(plays$scoringPlay) & plays$scoringPlay == TRUE, ]
  
  if(nrow(scoring_plays) == 0) {
    return(list(success = FALSE, error = "No scoring plays found"))
  }
  
  # Get first scorer
  first_play <- scoring_plays[1, ]
  first_text <- tolower(first_play$text)
  first_scorer <- if(grepl(tolower(team_name), first_text)) team_name else "Opponent"
  
  # Get winner from header
  winner <- NA
  if(!is.null(game_data$header$competitions)) {
    comp <- game_data$header$competitions
    competitors <- comp$competitors[[1]]
    away_score <- as.numeric(competitors$score[1])
    home_score <- as.numeric(competitors$score[2])
    
    if(grepl(team_name, competitors$team.displayName[1], ignore.case = TRUE)) {
      winner <- if(away_score > home_score) team_name else "Opponent"
    } else {
      winner <- if(home_score > away_score) team_name else "Opponent"
    }
  }
  
  return(list(
    success = TRUE,
    first_scorer = first_scorer,
    winner = winner,
    bet_won = (first_scorer == team_name && winner == team_name)
  ))
}

# Resolve NBA quarter total points
resolve_nba_quarter_total <- function(game_data, quarter_num, line_value, direction) {
  quarters <- get_nba_quarter_scores(game_data)
  if(is.null(quarters) || quarter_num > length(quarters)) {
    return(list(success = FALSE, error = "Quarter data not available"))
  }
  
  total <- quarters[[quarter_num]]$total
  line <- as.numeric(line_value)
  
  result <- list(
    success = TRUE,
    actual_value = total,
    line_value = line
  )
  
  if(direction == "over") {
    result$bet_won <- total > line
  } else if(direction == "under") {
    result$bet_won <- total < line
  }
  
  return(result)
}

# Resolve NBA half total points
resolve_nba_half_total <- function(game_data, half_num, line_value, direction) {
  quarters <- get_nba_quarter_scores(game_data)
  if(is.null(quarters)) {
    return(list(success = FALSE, error = "Quarter data not available"))
  }
  
  if(half_num == 1) {
    total <- quarters[[1]]$total + quarters[[2]]$total
  } else {
    total <- quarters[[3]]$total + quarters[[4]]$total
  }
  
  line <- as.numeric(line_value)
  
  result <- list(
    success = TRUE,
    actual_value = total,
    line_value = line
  )
  
  if(direction == "over") {
    result$bet_won <- total > line
  } else if(direction == "under") {
    result$bet_won <- total < line
  }
  
  return(result)
}

# Resolve NHL period total goals
resolve_nhl_period_total <- function(game_data, period_num, line_value, direction) {
  periods <- get_nhl_period_scores(game_data)
  if(is.null(periods) || period_num > length(periods)) {
    return(list(success = FALSE, error = "Period data not available"))
  }
  
  total <- periods[[period_num]]$total
  line <- as.numeric(line_value)
  
  result <- list(
    success = TRUE,
    actual_value = total,
    line_value = line
  )
  
  if(direction == "over") {
    result$bet_won <- total > line
  } else if(direction == "under") {
    result$bet_won <- total < line
  }
  
  return(result)
}

# Resolve both teams to score
resolve_both_teams_score <- function(game_data) {
  if(!is.null(game_data$boxscore$teams)) {
    away_score <- as.numeric(game_data$boxscore$teams$score[1])
    home_score <- as.numeric(game_data$boxscore$teams$score[2])
    
    return(list(
      success = TRUE,
      both_scored = (away_score > 0 && home_score > 0),
      away_score = away_score,
      home_score = home_score
    ))
  }
  
  return(list(success = FALSE, error = "Score data not available"))
}

# Resolve player batting fantasy score (MLB)
resolve_batting_fantasy_score <- function(game_data, player_name) {
  # Get player stats
  player_stats <- get_espn_mlb_player_stats(game_data$game_id %||% "unknown", player_name)
  
  if(is.null(player_stats)) {
    return(list(success = FALSE, error = "Player not found"))
  }
  
  # Fantasy scoring typically: 
  # Singles = 1, Doubles = 2, Triples = 3, HR = 4
  # RBI = 1, Runs = 1, Walks = 1, Stolen Bases = 2
  # Strikeouts = -0.5
  
  singles <- player_stats$hits - (player_stats$doubles + player_stats$triples + player_stats$home_runs)
  
  fantasy_score <- 
    singles * 1 +
    player_stats$doubles * 2 +
    player_stats$triples * 3 +
    player_stats$home_runs * 4 +
    player_stats$rbi * 1 +
    player_stats$runs * 1 +
    player_stats$walks * 1 +
    player_stats$stolen_bases * 2 +
    player_stats$strikeouts * -0.5
  
  return(list(
    success = TRUE,
    actual_value = fantasy_score,
    player = player_stats$player,
    breakdown = list(
      singles = singles,
      doubles = player_stats$doubles,
      triples = player_stats$triples,
      home_runs = player_stats$home_runs,
      rbi = player_stats$rbi,
      runs = player_stats$runs,
      walks = player_stats$walks,
      stolen_bases = player_stats$stolen_bases,
      strikeouts = player_stats$strikeouts
    )
  ))
}

# Resolve player dunks (NBA)
resolve_player_dunks <- function(game_data, player_name) {
  if(is.null(game_data$plays)) {
    return(list(success = FALSE, error = "No play-by-play data"))
  }
  
  # Search for dunks involving the player
  player_plays <- game_data$plays[grepl(player_name, game_data$plays$text, ignore.case = TRUE), ]
  dunk_plays <- player_plays[grepl("dunk", player_plays$text, ignore.case = TRUE), ]
  
  dunks <- nrow(dunk_plays)
  
  return(list(
    success = TRUE,
    actual_value = dunks,
    dunk_plays = dunk_plays$text
  ))
}

# Resolve player Hits + Runs + RBIs (MLB)
resolve_player_hits_runs_rbi <- function(game_data, player_name) {
  player_stats <- get_espn_mlb_player_stats(game_data$game_id %||% "unknown", player_name)
  
  if(is.null(player_stats)) {
    return(list(success = FALSE, error = "Player not found"))
  }
  
  total <- player_stats$hits + player_stats$runs + player_stats$rbi
  
  return(list(
    success = TRUE,
    actual_value = total,
    hits = player_stats$hits,
    runs = player_stats$runs,
    rbi = player_stats$rbi
  ))
}

# Get team inning totals for various stats
get_team_inning_totals <- function(game_data, team_name, innings, stat_type) {
  if(is.null(game_data$plays)) {
    return(NULL)
  }
  
  # Get scoring plays for the specified innings
  inning_plays <- game_data$plays[game_data$plays$period %in% 1:innings, ]
  
  # Determine which team
  is_home <- grepl(team_name, game_data$boxscore$teams$team.displayName[2], ignore.case = TRUE)
  
  total <- 0
  
  if(stat_type == "batting_walks") {
    # Count walks (BB) from batting team
    team_plays <- inning_plays[grepl(team_name, inning_plays$text, ignore.case = TRUE), ]
    total <- sum(grepl("walked|bb|base on balls", team_plays$text, ignore.case = TRUE))
    
  } else if(stat_type == "pitching_strikeouts") {
    # Count strikeouts thrown by pitching team
    team_plays <- inning_plays[grepl(team_name, inning_plays$text, ignore.case = TRUE), ]
    total <- sum(grepl("struck out|strikeout|k\\s+swinging|k\\s+looking", team_plays$text, ignore.case = TRUE))
    
  } else if(stat_type == "batters") {
    # Count batters faced in specific innings
    # This requires parsing plate appearances
    if(stat_type == "batters") {
      # Count unique at-bats in the inning
      at_bats <- unique(inning_plays$atBatId[!is.na(inning_plays$atBatId)])
      total <- length(at_bats)
    }
    
  } else if(stat_type == "pitches_thrown") {
    # Count pitches thrown in specific innings
    # Look for pitch events
    pitch_plays <- inning_plays[!is.na(inning_plays$pitchCount), ]
    total <- sum(pitch_plays$pitchCount, na.rm = TRUE)
    
  } else if(stat_type == "strikeouts_thrown") {
    # Count strikeouts thrown by pitching team
    team_plays <- inning_plays[grepl(team_name, inning_plays$text, ignore.case = TRUE), ]
    total <- sum(grepl("struck out", team_plays$text, ignore.case = TRUE))
  }
  
  return(total)
}

# Resolve team inning total batting walks
resolve_team_inning_batting_walks <- function(game_data, team_name, innings) {
  total <- get_team_inning_totals(game_data, team_name, innings, "batting_walks")
  
  return(list(
    success = TRUE,
    actual_value = total,
    team = team_name,
    innings = innings
  ))
}

# Resolve team inning total pitching strikeouts
resolve_team_inning_pitching_strikeouts <- function(game_data, team_name, innings) {
  total <- get_team_inning_totals(game_data, team_name, innings, "pitching_strikeouts")
  
  return(list(
    success = TRUE,
    actual_value = total,
    team = team_name,
    innings = innings
  ))
}

# Resolve team 1st inning total batters
resolve_team_inning_batters <- function(game_data, team_name) {
  if(is.null(game_data$plays)) {
    return(list(success = FALSE, error = "No play-by-play data"))
  }
  
  # Get 1st inning plays
  inning_1_plays <- game_data$plays[game_data$plays$period == 1, ]
  
  # Count unique at-bats for the team
  team_plays <- inning_1_plays[grepl(team_name, inning_1_plays$text, ignore.case = TRUE), ]
  at_bats <- unique(team_plays$atBatId[!is.na(team_plays$atBatId)])
  total <- length(at_bats)
  
  return(list(
    success = TRUE,
    actual_value = total,
    team = team_name
  ))
}

# Resolve team 1st inning total pitches thrown
resolve_team_inning_pitches <- function(game_data, team_name) {
  if(is.null(game_data$plays)) {
    return(list(success = FALSE, error = "No play-by-play data"))
  }
  
  # Get 1st inning plays
  inning_1_plays <- game_data$plays[game_data$plays$period == 1, ]
  
  # Sum pitch counts
  total <- sum(inning_1_plays$pitchCount, na.rm = TRUE)
  
  return(list(
    success = TRUE,
    actual_value = total,
    team = team_name
  ))
}

# Resolve team 1st inning total strikeouts thrown
resolve_team_inning_strikeouts_thrown <- function(game_data, team_name) {
  if(is.null(game_data$plays)) {
    return(list(success = FALSE, error = "No play-by-play data"))
  }
  
  # Get 1st inning plays
  inning_1_plays <- game_data$plays[game_data$plays$period == 1, ]
  
  # Count strikeouts by the pitching team
  team_plays <- inning_1_plays[grepl(team_name, inning_1_plays$text, ignore.case = TRUE), ]
  strikeouts <- sum(grepl("struck out", team_plays$text, ignore.case = TRUE))
  
  return(list(
    success = TRUE,
    actual_value = strikeouts,
    team = team_name
  ))
}

# Resolve team player first basket (NBA)
resolve_team_player_first_basket <- function(game_data, team_name) {
  if(is.null(game_data$plays)) {
    return(list(success = FALSE, error = "No play-by-play data"))
  }
  
  # Find first scoring play of the game
  scoring_plays <- game_data$plays[!is.na(game_data$plays$scoringPlay) & game_data$plays$scoringPlay == TRUE, ]
  
  if(nrow(scoring_plays) == 0) {
    return(list(success = FALSE, error = "No scoring plays found"))
  }
  
  first_play <- scoring_plays[1, ]
  play_text <- first_play$text
  
  # Check if the scoring team matches
  if(grepl(team_name, play_text, ignore.case = TRUE)) {
    # Extract scorer name from play text
    scorer <- "Unknown"
    # Pattern: "Player Name makes/made X point shot"
    if(grepl("makes|made", play_text)) {
      name_part <- strsplit(play_text, " makes | made ")[[1]][1]
      scorer <- trimws(name_part)
    }
    
    return(list(
      success = TRUE,
      player = scorer,
      team = team_name,
      points = first_play$scoreValue %||% 2,
      description = play_text
    ))
  }
  
  return(list(
    success = TRUE,
    player = NULL,
    team = team_name,
    scored_first = FALSE,
    description = play_text
  ))
}


# Function to resolve multi-inning markets (generic)
resolve_multi_inning_market <- function(game_id, num_innings, market_subtype, line_value, direction, team = NULL) {
  result <- list(success = FALSE, actual_value = 0, bet_won = NULL, error = NULL)
  
  tryCatch({
    # For total_runs, use the specific function
    if (market_subtype == "total_runs") {
      # Fetch game data
      url <- paste0("https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary?event=", game_id)
      response <- GET(url, timeout = 10)
      if (status_code(response) == 200) {
        game_data <- fromJSON(rawToChar(response$content))
        result <- resolve_multi_inning_total_runs(game_data, num_innings, line_value, direction)
      } else {
        result$error <- "Failed to fetch game data"
      }
    } else {
      # For other market types, return not implemented
      result$error <- paste("Multi-inning market type not yet implemented:", market_subtype)
    }
  }, error = function(e) {
    result$error <- paste("Error in resolve_multi_inning_market:", e$message)
  })
  
  return(result)
}

# Fix for NHL first/last scorer
resolve_nhl_first_last_scorer <- function(game_id, player_name, is_first = TRUE) {
  result <- list(success = FALSE, player_scored = FALSE, scorer = NULL, error = NULL)
  
  tryCatch({
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary?event=", game_id)
    response <- GET(url, timeout = 10)
    
    if (status_code(response) == 200) {
      data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
      
      if (!is.null(data$plays) && is.data.frame(data$plays) && nrow(data$plays) > 0) {
        # Find goal plays
        goal_plays <- data$plays[grepl("goal", data$plays$text, ignore.case = TRUE), ]
        
        if (nrow(goal_plays) > 0) {
          if (is_first) {
            target_play <- goal_plays[which.min(goal_plays$id), ]
          } else {
            target_play <- goal_plays[which.max(goal_plays$id), ]
          }
          
          # Extract scorer from play text
          play_text <- tolower(target_play$text)
          result$scorer <- target_play$text
          
          # Check if player scored (handle partial matches)
          player_lower <- tolower(player_name)
          result$player_scored <- grepl(player_lower, play_text, fixed = TRUE) ||
            grepl(gsub("^\\w+\\s+", "", player_lower), play_text, fixed = TRUE)
          
          result$success <- TRUE
          debug_cat(sprintf("  %s goal scorer: %s\n", if(is_first) "First" else "Last", result$scorer))
          debug_cat(sprintf("  Player %s scored: %s\n", player_name, result$player_scored))
        } else {
          result$error <- "No goals found in this game"
          debug_cat("  No goals found in game\n")
        }
      } else {
        result$error <- "No play-by-play data available"
      }
    } else {
      result$error <- paste("API returned status:", status_code(response))
    }
  }, error = function(e) {
    result$error <- paste("Error in resolve_nhl_first_last_scorer:", e$message)
  })
  
  return(result)
}

# ==============================================
# MLB TOTAL MARKETS RESOLVERS
# ==============================================

resolve_mlb_total_strikeouts <- function(game_id, line_value, direction, player_name = NULL) {
  debug_cat(sprintf("  Resolving MLB total strikeouts: %s %s\n", direction, line_value))
  
  result <- list(success = FALSE, actual_value = 0, bet_won = NULL, error = NULL)
  
  tryCatch({
    # Fetch game data from ESPN
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary?event=", game_id)
    response <- GET(url, timeout = 10)
    
    if (status_code(response) != 200) {
      result$error <- paste("ESPN API returned status:", status_code(response))
      return(result)
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    total_strikeouts <- 0
    
    # Sum strikeouts from both teams
    if (!is.null(data$boxscore$teams)) {
      teams_data <- data$boxscore$teams
      
      for (i in 1:nrow(teams_data)) {
        team <- teams_data[i, ]
        
        # Look for strikeout stats in player statistics
        if (!is.null(team$statistics) && length(team$statistics) > 0) {
          stats_list <- team$statistics[[1]]
          
          if (is.data.frame(stats_list)) {
            for (j in 1:nrow(stats_list)) {
              stat_group <- stats_list[j, ]
              
              # Look for pitching stats (strikeouts)
              if (!is.null(stat_group$type) && stat_group$type == "pitching") {
                if (!is.null(stat_group$labels) && !is.null(stat_group$athletes)) {
                  labels <- stat_group$labels[[1]]
                  athletes <- stat_group$athletes[[1]]
                  
                  if (is.data.frame(athletes)) {
                    for (k in 1:nrow(athletes)) {
                      athlete <- athletes[k, ]
                      if (!is.null(athlete$stats) && is.list(athlete$stats) && length(athlete$stats) > 0) {
                        stats_vector <- athlete$stats[[1]]
                        
                        # Find strikeouts index (usually "K" or "strikeouts" in labels)
                        for (l in seq_along(labels)) {
                          label <- tolower(labels[l])
                          if (label %in% c("k", "strikeouts", "so")) {
                            total_strikeouts <- total_strikeouts + as.numeric(stats_vector[l] %||% 0)
                            break
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    
    result$success <- TRUE
    result$actual_value <- total_strikeouts
    
    if (!is.null(line_value) && !is.null(direction)) {
      line_num <- as.numeric(line_value)
      if (direction == "over") {
        result$bet_won <- total_strikeouts > line_num
      } else if (direction == "under") {
        result$bet_won <- total_strikeouts < line_num
      }
    }
    
    debug_cat(sprintf("  Total strikeouts: %d\n", total_strikeouts))
    
  }, error = function(e) {
    result$error <- paste("Error in resolve_mlb_total_strikeouts:", e$message)
  })
  
  return(result)
}

resolve_mlb_total_walks <- function(game_id, line_value, direction, player_name = NULL) {
  debug_cat(sprintf("  Resolving MLB total walks: %s %s\n", direction, line_value))
  
  result <- list(success = FALSE, actual_value = 0, bet_won = NULL, error = NULL)
  
  tryCatch({
    # Fetch game data from ESPN
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary?event=", game_id)
    response <- GET(url, timeout = 10)
    
    if (status_code(response) != 200) {
      result$error <- paste("ESPN API returned status:", status_code(response))
      return(result)
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    total_walks <- 0
    
    # Sum walks from both teams
    if (!is.null(data$boxscore$teams)) {
      teams_data <- data$boxscore$teams
      
      for (i in 1:nrow(teams_data)) {
        team <- teams_data[i, ]
        
        # Look for walk stats
        if (!is.null(team$statistics) && length(team$statistics) > 0) {
          stats_list <- team$statistics[[1]]
          
          if (is.data.frame(stats_list)) {
            for (j in 1:nrow(stats_list)) {
              stat_group <- stats_list[j, ]
              
              # Look for batting stats (walks)
              if (!is.null(stat_group$type) && stat_group$type == "batting") {
                if (!is.null(stat_group$labels) && !is.null(stat_group$athletes)) {
                  labels <- stat_group$labels[[1]]
                  athletes <- stat_group$athletes[[1]]
                  
                  if (is.data.frame(athletes)) {
                    for (k in 1:nrow(athletes)) {
                      athlete <- athletes[k, ]
                      if (!is.null(athlete$stats) && is.list(athlete$stats) && length(athlete$stats) > 0) {
                        stats_vector <- athlete$stats[[1]]
                        
                        # Find walks index (usually "BB" or "walks" in labels)
                        for (l in seq_along(labels)) {
                          label <- tolower(labels[l])
                          if (label %in% c("bb", "walks")) {
                            total_walks <- total_walks + as.numeric(stats_vector[l] %||% 0)
                            break
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    
    result$success <- TRUE
    result$actual_value <- total_walks
    
    if (!is.null(line_value) && !is.null(direction)) {
      line_num <- as.numeric(line_value)
      if (direction == "over") {
        result$bet_won <- total_walks > line_num
      } else if (direction == "under") {
        result$bet_won <- total_walks < line_num
      }
    }
    
    debug_cat(sprintf("  Total walks: %d\n", total_walks))
    
  }, error = function(e) {
    result$error <- paste("Error in resolve_mlb_total_walks:", e$message)
  })
  
  return(result)
}

resolve_mlb_total_rbis <- function(game_id, line_value, direction, player_name = NULL) {
  debug_cat(sprintf("  Resolving MLB total RBIs: %s %s\n", direction, line_value))
  
  result <- list(success = FALSE, actual_value = 0, bet_won = NULL, error = NULL)
  
  tryCatch({
    # Fetch game data from ESPN
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary?event=", game_id)
    response <- GET(url, timeout = 10)
    
    if (status_code(response) != 200) {
      result$error <- paste("ESPN API returned status:", status_code(response))
      return(result)
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    total_rbis <- 0
    
    # Sum RBIs from both teams
    if (!is.null(data$boxscore$teams)) {
      teams_data <- data$boxscore$teams
      
      for (i in 1:nrow(teams_data)) {
        team <- teams_data[i, ]
        
        # Look for RBI stats
        if (!is.null(team$statistics) && length(team$statistics) > 0) {
          stats_list <- team$statistics[[1]]
          
          if (is.data.frame(stats_list)) {
            for (j in 1:nrow(stats_list)) {
              stat_group <- stats_list[j, ]
              
              # Look for batting stats (RBIs)
              if (!is.null(stat_group$type) && stat_group$type == "batting") {
                if (!is.null(stat_group$labels) && !is.null(stat_group$athletes)) {
                  labels <- stat_group$labels[[1]]
                  athletes <- stat_group$athletes[[1]]
                  
                  if (is.data.frame(athletes)) {
                    for (k in 1:nrow(athletes)) {
                      athlete <- athletes[k, ]
                      if (!is.null(athlete$stats) && is.list(athlete$stats) && length(athlete$stats) > 0) {
                        stats_vector <- athlete$stats[[1]]
                        
                        # Find RBIs index (usually "RBI" or "rbi" in labels)
                        for (l in seq_along(labels)) {
                          label <- tolower(labels[l])
                          if (label %in% c("rbi", "rbis", "runs batted in")) {
                            total_rbis <- total_rbis + as.numeric(stats_vector[l] %||% 0)
                            break
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    
    result$success <- TRUE
    result$actual_value <- total_rbis
    
    if (!is.null(line_value) && !is.null(direction)) {
      line_num <- as.numeric(line_value)
      if (direction == "over") {
        result$bet_won <- total_rbis > line_num
      } else if (direction == "under") {
        result$bet_won <- total_rbis < line_num
      }
    }
    
    debug_cat(sprintf("  Total RBIs: %d\n", total_rbis))
    
  }, error = function(e) {
    result$error <- paste("Error in resolve_mlb_total_rbis:", e$message)
  })
  
  return(result)
}

# ==============================================
# NHL GAME TOTAL MARKETS RESOLVERS
# ==============================================

resolve_nhl_game_total_goals <- function(game_id, line_value, direction, is_oddeven = FALSE) {
  debug_cat(sprintf("  Resolving NHL game total goals: %s %s\n", direction, line_value))
  
  result <- list(success = FALSE, actual_value = 0, bet_won = NULL, error = NULL)
  
  tryCatch({
    period_data <- get_espn_nhl_period_data(game_id)
    
    if (!period_data$success) {
      result$error <- period_data$error
      return(result)
    }
    
    total_goals <- period_data$game_data$home_score + period_data$game_data$away_score
    result$actual_value <- total_goals
    result$success <- TRUE
    
    if (is_oddeven) {
      result$bet_won <- (total_goals %% 2 == 1)  # Odd = 1, Even = 0
      debug_cat(sprintf("  Total goals: %d - %s\n", total_goals, ifelse(result$bet_won, "ODD", "EVEN")))
    } else if (!is.null(line_value) && !is.null(direction)) {
      line_num <- as.numeric(line_value)
      if (direction == "over") {
        result$bet_won <- total_goals > line_num
      } else if (direction == "under") {
        result$bet_won <- total_goals < line_num
      }
      debug_cat(sprintf("  Total goals: %d (line: %.1f) - %s\n", total_goals, line_num, 
                        ifelse(result$bet_won, "WIN", "LOSS")))
    }
    
  }, error = function(e) {
    result$error <- paste("Error in resolve_nhl_game_total_goals:", e$message)
  })
  
  return(result)
}

resolve_nhl_game_total_shots <- function(game_id, line_value, direction) {
  debug_cat(sprintf("  Resolving NHL game total shots: %s %s\n", direction, line_value))
  
  result <- list(success = FALSE, actual_value = 0, bet_won = NULL, error = NULL)
  
  tryCatch({
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary?event=", game_id)
    response <- GET(url, timeout = 10)
    
    if (status_code(response) != 200) {
      result$error <- paste("ESPN API returned status:", status_code(response))
      return(result)
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    total_shots <- 0
    
    # Sum shots on goal from both teams
    if (!is.null(data$boxscore$teams)) {
      teams_data <- data$boxscore$teams
      
      for (i in 1:nrow(teams_data)) {
        team <- teams_data[i, ]
        
        # Look for shot stats in team statistics
        if (!is.null(team$statistics) && length(team$statistics) > 0) {
          stats_list <- team$statistics[[1]]
          
          if (is.data.frame(stats_list)) {
            for (j in 1:nrow(stats_list)) {
              stat_group <- stats_list[j, ]
              
              if (!is.null(stat_group$type) && stat_group$type == "skaters") {
                if (!is.null(stat_group$labels) && is.data.frame(stat_group$labels)) {
                  labels <- stat_group$labels[[1]]
                  
                  # Find shots index (usually "SOG" or "sog" in labels)
                  for (k in seq_along(labels)) {
                    label <- tolower(labels[k])
                    if (label %in% c("sog", "shots on goal", "shots")) {
                      if (!is.null(team[[paste0("statistics.", j, ".teamStats")]])) {
                        team_stats <- team[[paste0("statistics.", j, ".teamStats")]]
                        total_shots <- total_shots + as.numeric(team_stats[k] %||% 0)
                      }
                      break
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    
    # Fallback: Try to get shots from boxscore directly
    if (total_shots == 0 && !is.null(data$boxscore$teams)) {
      teams_data <- data$boxscore$teams
      for (i in 1:nrow(teams_data)) {
        team <- teams_data[i, ]
        if (!is.null(team$statistics) && is.list(team$statistics) && length(team$statistics) > 0) {
          team_stats <- team$statistics[[1]]
          if (is.list(team_stats) && !is.null(team_stats$shotsOnGoal)) {
            total_shots <- total_shots + as.numeric(team_stats$shotsOnGoal %||% 0)
          }
        }
      }
    }
    
    result$success <- TRUE
    result$actual_value <- total_shots
    
    if (!is.null(line_value) && !is.null(direction)) {
      line_num <- as.numeric(line_value)
      if (direction == "over") {
        result$bet_won <- total_shots > line_num
      } else if (direction == "under") {
        result$bet_won <- total_shots < line_num
      }
    }
    
    debug_cat(sprintf("  Total shots on goal: %d\n", total_shots))
    
  }, error = function(e) {
    result$error <- paste("Error in resolve_nhl_game_total_shots:", e$message)
  })
  
  return(result)
}

resolve_nhl_game_power_play_goals <- function(game_id, line_value, direction) {
  debug_cat(sprintf("  Resolving NHL game power play goals: %s %s\n", direction, line_value))
  
  result <- list(success = FALSE, actual_value = 0, bet_won = NULL, error = NULL)
  
  tryCatch({
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary?event=", game_id)
    response <- GET(url, timeout = 10)
    
    if (status_code(response) != 200) {
      result$error <- paste("ESPN API returned status:", status_code(response))
      return(result)
    }
    
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    
    total_pp_goals <- 0
    
    # Parse plays to count power play goals
    if (!is.null(data$plays) && is.data.frame(data$plays) && nrow(data$plays) > 0) {
      # Look for goal plays with power play indicator
      for (i in 1:nrow(data$plays)) {
        play <- data$plays[i, ]
        play_text <- tolower(play$text %||% "")
        
        # Check if it's a goal and power play
        if (grepl("goal", play_text) && (grepl("power play", play_text) || grepl("pp", play_text))) {
          total_pp_goals <- total_pp_goals + 1
        }
      }
    }
    
    result$success <- TRUE
    result$actual_value <- total_pp_goals
    
    if (!is.null(line_value) && !is.null(direction)) {
      line_num <- as.numeric(line_value)
      if (direction == "over") {
        result$bet_won <- total_pp_goals > line_num
      } else if (direction == "under") {
        result$bet_won <- total_pp_goals < line_num
      }
    }
    
    debug_cat(sprintf("  Total power play goals: %d\n", total_pp_goals))
    
  }, error = function(e) {
    result$error <- paste("Error in resolve_nhl_game_power_play_goals:", e$message)
  })
  
  return(result)
}

resolve_bet <- function(player_name, sport, season, market_type, event_string, line_value = NULL, game_date = NULL) {
  debug_cat("\n===============================================================================\n")
  debug_cat("STARTING BET RESOLUTION - DEBUG VERSION\n")
  debug_cat("===============================================================================\n")
  debug_cat(sprintf("Market type received from Python: '%s'\n", market_type))
  debug_cat(sprintf("Player name: '%s'\n", player_name))
  debug_cat(sprintf("Full line: '%s'\n", paste(player_name, market_type)))
  debug_cat(sprintf("Line value: %s\n", line_value))
  debug_cat(sprintf("Event: %s\n", event_string))
  debug_cat(sprintf("Game date: %s\n", game_date))
  
  # ==========================================================================
  # TEMPORARY FIX: Override broken Python classification
  # ==========================================================================
  original_player_name <- player_name
  market_lower <- tolower(market_type)
  player_lower <- tolower(player_name)
  
  # ==========================================================================
  # FIX 0: Handle mangled player names from Python classifier
  # ==========================================================================
  debug_cat("  Checking for mangled player names...\n")
  
  if (player_name == "ton Dach" || player_lower == "ton dach") {
    debug_cat("  Fixing mangled player name: 'ton Dach' -> 'Colton Dach'\n")
    player_name <- "Colton Dach"
    player_lower <- tolower(player_name)
  }
  
  if (grepl("^cago\\s", player_name, ignore.case = TRUE)) {
    debug_cat("  Fixing mangled name starting with 'cago'\n")
    player_name <- gsub("^cago\\s*(Blackhawks)?\\s*", "", player_name, ignore.case = TRUE)
    player_name <- trimws(player_name)
    player_lower <- tolower(player_name)
    debug_cat(sprintf("  Fixed to: '%s'\n", player_name))
  }
  
  short_abbrs <- c("Chi", "Col", "Bos", "NY", "LA", "Det", "Tor", "Van", "Cal", "Edm",
                   "chi", "col", "bos", "ny", "la", "det", "tor", "van", "cal", "edm")
  if (player_name %in% short_abbrs) {
    debug_cat(sprintf("  WARNING: Player name '%s' looks like a team abbreviation!\n", player_name))
    debug_cat("  Cannot auto-fix - need proper player name\n")
  }
  
  mangled_prefixes <- list(
    "^ton\\s" = "Col", "^gie\\s" = "Dou", "^ugie\\s" = "Do",
    "^nnor\\s" = "Co", "^than\\s" = "Na", "^ex\\s" = "Al"
  )
  
  for (prefix_pattern in names(mangled_prefixes)) {
    if (grepl(prefix_pattern, player_name, ignore.case = TRUE)) {
      fix_prefix <- mangled_prefixes[[prefix_pattern]]
      fixed_name <- gsub(prefix_pattern, paste0(fix_prefix, ""), player_name, ignore.case = TRUE)
      debug_cat(sprintf("  Fixing mangled prefix: '%s' -> '%s'\n", player_name, fixed_name))
      player_name <- fixed_name
      player_lower <- tolower(player_name)
      break
    }
  }
  
  # ==========================================================================
  # FIX 1: Fix NHL scorer markets (ONLY override market_type, no resolution here)
  # ==========================================================================
  if (sport %in% c("nhl", "hockey")) {
    debug_cat("  NHL game detected, checking for scorer markets...\n")
    
    # FIRST: Check for specific shot types before generic "shots"
    if (grepl("blocked shots|blocks", market_lower)) {
      debug_cat("  OVERRIDE: Keeping 'Player Blocked Shots' for NHL\n")
      market_type <- "Player Blocked Shots"
      
    } else if (grepl("team.*player.*first.*goal.*scorer", market_lower) ||
               grepl("first.*goal.*scorer", market_lower) ||
               (market_lower == "generic" && grepl("blackhawks|jets|devils|bruins|rangers|maple leafs|canadiens|penguins|capitals|lightning|panthers|hurricanes|blue jackets|red wings|sabres|senators|islanders|flyers|wild|predators|blues|stars|avalanche|coyotes|flames|oilers|canucks|kraken|golden knights|ducks|kings|sharks", player_lower))) {
      debug_cat("  OVERRIDE: NHL scorer market detected\n")
      market_type <- "Player First Goal"
      
      nhl_teams <- c("chicago blackhawks", "winnipeg jets", "new jersey devils", "boston bruins",
                     "new york rangers", "toronto maple leafs", "montreal canadiens", "pittsburgh penguins",
                     "washington capitals", "tampa bay lightning", "florida panthers", "carolina hurricanes",
                     "columbus blue jackets", "detroit red wings", "buffalo sabres", "ottawa senators",
                     "new york islanders", "philadelphia flyers", "minnesota wild", "nashville predators",
                     "st louis blues", "dallas stars", "colorado avalanche", "arizona coyotes",
                     "calgary flames", "edmonton oilers", "vancouver canucks", "seattle kraken",
                     "vegas golden knights", "anaheim ducks", "los angeles kings", "san jose sharks")
      
      for (team in nhl_teams) {
        if (grepl(team, player_lower, fixed = TRUE)) {
          player_name <- gsub(paste0("(?i)", team, "\\s*"), "", player_name)
          player_name <- trimws(player_name)
          debug_cat(sprintf("  Fixed player name: '%s'\n", player_name))
          break
        }
      }
    } else if (grepl("player.*first.*goal", market_lower)) {
      debug_cat("  OVERRIDE: Keeping 'Player First Goal'\n")
      market_type <- "Player First Goal"
    } else if (market_lower == "generic") {
      debug_cat("  OVERRIDE: NHL 'generic' market -> assuming 'Player First Goal'\n")
      market_type <- "Player First Goal"
    } else if (grepl("player.*assists", market_lower)) {
      debug_cat("  OVERRIDE: Keeping 'Player Assists' for NHL\n")
      market_type <- "Player Assists"
    } else if (grepl("player.*goals", market_lower)) {
      debug_cat("  OVERRIDE: Keeping 'Player Goals' for NHL\n")
      market_type <- "Player Goals"
    } else if (grepl("player.*saves", market_lower)) {
      debug_cat("  OVERRIDE: Keeping 'Player Saves' for NHL\n")
      market_type <- "Player Saves"
    } else if (grepl("player.*points", market_lower)) {
      debug_cat("  OVERRIDE: Keeping 'Player Points' for NHL\n")
      market_type <- "Player Points"
    } else if (grepl("player.*shots", market_lower)) {
      debug_cat("  OVERRIDE: Keeping 'Player Shots On Goal' for NHL\n")
      market_type <- "Player Shots On Goal"
    } else if (grepl("penalty minutes|pim", market_lower)) {
      debug_cat("  OVERRIDE: Keeping 'Player Penalty Minutes' for NHL\n")
      market_type <- "Player Penalty Minutes"
    } else if (grepl("time on ice|toi", market_lower)) {
      debug_cat("  OVERRIDE: Keeping 'Player Time On Ice' for NHL\n")
      market_type <- "Player Time On Ice"
    } else if (grepl("hits", market_lower)) {
      debug_cat("  OVERRIDE: Keeping 'Player Hits' for NHL\n")
      market_type <- "Player Hits"
    }
  }
  
  # Fix combo markets (for all sports)
  if (grepl("points.*\\+.*assists|points.*assists", market_lower) && !grepl("rebounds", market_lower)) {
    debug_cat("  OVERRIDE: Setting market to 'Player Points + Assists'\n")
    market_type <- "Player Points + Assists"
  }
  
  if (grepl("rebounds.*\\+.*assists|rebounds.*assists", market_lower) && !grepl("points", market_lower)) {
    debug_cat("  OVERRIDE: Setting market to 'Player Rebounds + Assists'\n")
    market_type <- "Player Rebounds + Assists"
  }
  
  if (grepl("points.*rebounds.*assists|pra|p\\+r\\+a", market_lower)) {
    debug_cat("  OVERRIDE: Setting market to 'Player Points + Rebounds + Assists'\n")
    market_type <- "Player Points + Rebounds + Assists"
  }
  
  if (grepl("points.*\\+.*rebounds|points.*rebounds", market_lower) && !grepl("assists", market_lower)) {
    debug_cat("  OVERRIDE: Setting market to 'Player Points + Rebounds'\n")
    market_type <- "Player Points + Rebounds"
  }
  
  if (grepl("blocks.*\\+.*steals|blocks.*steals|steals.*\\+.*blocks|steals.*blocks", market_lower)) {
    debug_cat("  OVERRIDE: Setting market to 'Player Blocks + Steals'\n")
    market_type <- "Player Blocks + Steals"
  }
  
  debug_cat(sprintf("  Final market type: '%s'\n", market_type))
  debug_cat(sprintf("  Final player name: '%s'\n", player_name))
  
  result <- list(
    success = FALSE,
    resolved = FALSE,
    bet_type = market_type,
    player = player_name,
    sport = sport,
    original_input_season = season,
    market = market_type,
    event = event_string,
    data = NULL,
    error = NULL
  )
  
  # ========== FUTURE GAME CHECK ==========
  game_date_obj <- extract_date_from_event(event_string, game_date)
  current_date <- Sys.Date()
  
  if (game_date_obj > current_date) {
    result <- list(
      success = FALSE,
      resolved = FALSE,
      future_game = TRUE,
      game_date = as.character(game_date_obj),
      current_date = as.character(current_date),
      error = paste("Game is in the future and cannot be resolved yet.",
                    "Game date:", game_date_obj, "Today:", current_date),
      note = "Future games will be resolved when the game occurs"
    )
    debug_cat(sprintf("❌ FUTURE GAME DETECTED: %s > %s\n", game_date_obj, current_date))
    return(result)
  }
  
  tryCatch({
    sport <- normalize_sport_name(sport)

    # CRITICAL: MLB team override - if event contains MLB teams, force sport to "mlb"
    if (sport != "mlb" && check_mlb_team_in_event(event_string)) {
      debug_cat(sprintf("  SPORT OVERRIDE: '%s' -> 'mlb' (MLB team found in event: '%s')\n", sport, event_string))
      sport <- "mlb"
      result$sport <- "mlb"
      result$sport_overridden <- TRUE
    }

    # NCAA check for NFL
    if (sport == "nfl" || sport == "football") {
      ncaa_team_keywords <- c(
        "Oregon", "Texas Tech", "Alabama", "Clemson", "Ohio State",
        "Michigan", "USC", "Notre Dame", "UCLA", "Stanford",
        "Florida State", "Georgia", "LSU", "Oklahoma", "Texas",
        "Penn State", "Wisconsin", "Iowa", "Florida", "Auburn",
        "Tennessee", "Arkansas", "Mississippi", "Miss State", "Kentucky",
        "South Carolina", "Missouri", "Texas A&M", "Baylor", "TCU"
      )
      
      event_upper <- toupper(event_string)
      has_ncaa_team <- FALSE
      ncaa_team_found <- NULL
      
      for (team in ncaa_team_keywords) {
        team_upper <- toupper(team)
        if (grepl(paste0("\\b", team_upper, "\\b"), event_upper) ||
            grepl(paste0(team_upper, " "), event_upper) ||
            grepl(paste0(" ", team_upper, "\\b"), event_upper)) {
          has_ncaa_team <- TRUE
          ncaa_team_found <- team
          break
        }
      }
      
      if (has_ncaa_team) {
        error_msg <- sprintf("NCAA (college) game detected. '%s' is a college team. Only NFL games are supported.", ncaa_team_found)
        debug_cat(sprintf("ERROR: %s\n", error_msg))
        result$error <- error_msg
        return(result)
      }
    }
    
    debug_cat("\n--- Step 1: Extracting game information ---\n")
    debug_cat(sprintf("Extracted date: %s (from provided: %s)\n", game_date_obj, game_date))
    
    teams <- extract_teams_from_event(event_string, sport)
    
    if (is.null(teams)) {
      error_msg <- "Could not extract teams from event string"
      debug_cat(sprintf("ERROR: %s\n", error_msg))
      result$error <- error_msg
      return(result)
    }
    
    debug_cat(sprintf("Extracted teams: %s @ %s (team_only: %s)\n",
                      teams$away, teams$home, teams$team_only %||% FALSE))
    
    if (!is.null(teams$team_only) && teams$team_only == TRUE) {
      debug_cat("  SPECIAL FORMAT DETECTED: Team-only case from extract_teams_from_event\n")
    }
    
    debug_cat("\n--- Step 2: Finding game ID ---\n")
    
    if (teams$home == "Unknown Opponent" || teams$away == "Unknown Opponent") {
      debug_cat("WARNING: One team is 'Unknown Opponent' - using team-only lookup mode\n")
      
      known_team <- if (teams$home != "Unknown Opponent") teams$home else teams$away
      debug_cat(sprintf("Known team: %s\n", known_team))
      
      if (sport %in% c("nba", "basketball")) {
        game_info <- find_nba_game_for_team_only(known_team, game_date_obj)
        if (!is.null(game_info) && !is.na(game_info$game_id)) {
          game_id <- game_info$game_id
          debug_cat(sprintf("  Using NBA team-only lookup for %s on %s\n", known_team, game_date_obj))
        } else {
          debug_cat("  NBA team-only lookup failed, using find_game_id with team_only_mode = TRUE\n")
          game_id <- find_game_id(teams$home, teams$away, game_date_obj, sport, team_only_mode = TRUE)
        }
      } else {
        game_id <- find_game_id(teams$home, teams$away, game_date_obj, sport, team_only_mode = TRUE)
      }
    } else {
      game_id <- find_game_id(teams$home, teams$away, game_date_obj, sport, team_only_mode = FALSE)
    }
    
    if (is.na(game_id) || is.null(game_id)) {
      error_msg <- "Could not find game ID"
      debug_cat(sprintf("ERROR: %s\n", error_msg))
      result$error <- error_msg
      return(result)
    }
    
    debug_cat(sprintf("Found game ID: %s\n", game_id))
    
    result$game_id <- game_id
    result$game_date <- as.character(game_date_obj)
    result$home_team <- teams$home
    result$away_team <- teams$away
    
    debug_cat("\n--- Step 3: Calculating correct season ---\n")
    
    result$calculated_season <- detect_correct_season(sport, game_date_obj)
    debug_cat(sprintf("Intelligent season detection: %s (input was: %s)\n",
                      result$calculated_season, season))
    
    season_to_use <- check_and_fix_season_availability(sport, result$calculated_season, game_date_obj)
    
    if (season_to_use != result$calculated_season) {
      debug_cat(sprintf("⚠️ Using fallback season: %s (calculated: %s)\n",
                        season_to_use, result$calculated_season))
      result$fallback_season_used <- TRUE
      result$original_calculated_season <- result$calculated_season
    } else {
      result$fallback_season_used <- FALSE
    }
    
    result$season <- season_to_use
    result$data_availability_checked <- TRUE
    debug_cat(sprintf("Final season to use: %s\n", result$season))
    
    market_lower <- tolower(market_type)
    debug_cat(sprintf("\n--- Step 4: Processing market type '%s' ---\n", market_type))
    
    market_info <- classify_market(market_type, sport)
    debug_cat(sprintf("Market classification: %s\n", market_info$type))
    
    # ========== FORCED OVERRIDE FOR NFL TEAM MARKETS ==========
    if (sport == "nfl" || sport == "football") {
      if (grepl("team to score first", tolower(market_type))) {
        debug_cat("  FORCED OVERRIDE: Setting market type to 'team_score_first' for NFL\n")
        market_info <- list(type = "team_score_first")
      }
      if (grepl("team score first and win", tolower(market_type))) {
        debug_cat("  FORCED OVERRIDE: Setting market type to 'team_score_first_and_win' for NFL\n")
        market_info <- list(type = "team_score_first_and_win")
      }
    }
    
    # Helper to fetch game data for resolution
    fetch_game_data_for_resolution <- function(game_id, sport) {
      tryCatch({
        if(sport == "mlb") {
          url <- paste0("https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary?event=", game_id)
        } else if(sport == "nba") {
          url <- paste0("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event=", game_id)
        } else if(sport == "nhl") {
          url <- paste0("https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary?event=", game_id)
        } else {
          return(NULL)
        }
        response <- GET(url, timeout = 10)
        if(status_code(response) == 200) {
          # CRITICAL: Use flatten = TRUE to get consistent data frame structure
          return(fromJSON(rawToChar(response$content), flatten = TRUE))
        }
        return(NULL)
      }, error = function(e) {
        debug_cat(sprintf("Error fetching game data: %s\n", e$message))
        return(NULL)
      })
    }
    
    # ==================== MLB TOTAL MARKETS ====================
    if (sport == "mlb" && grepl("total.*strikeouts", market_lower)) {
      result_data <- resolve_mlb_total_strikeouts(game_id, line_value, gsub("total.*strikeouts", "", market_lower), player_name)
      if (result_data$success) {
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- result_data
        if (!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
        if (!is.null(result_data$actual_value)) result$actual_value <- result_data$actual_value
      } else {
        result$error <- result_data$error
      }
      
    } else if (sport == "mlb" && grepl("total.*walks", market_lower)) {
      result_data <- resolve_mlb_total_walks(game_id, line_value, gsub("total.*walks", "", market_lower), player_name)
      if (result_data$success) {
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- result_data
        if (!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
        if (!is.null(result_data$actual_value)) result$actual_value <- result_data$actual_value
      } else {
        result$error <- result_data$error
      }
      
    } else if (sport == "mlb" && grepl("total.*rbis?", market_lower)) {
      result_data <- resolve_mlb_total_rbis(game_id, line_value, gsub("total.*rbis?", "", market_lower), player_name)
      if (result_data$success) {
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- result_data
        if (!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
        if (!is.null(result_data$actual_value)) result$actual_value <- result_data$actual_value
      } else {
        result$error <- result_data$error
      }
      
      # ==================== NHL GAME TOTAL MARKETS ====================
    } else if (sport == "nhl" && market_info$type == "game_total_goals") {
      result_data <- resolve_nhl_game_total_goals(game_id, line_value, if(grepl("over", market_lower)) "over" else if(grepl("under", market_lower)) "under" else NULL, FALSE)
      if (result_data$success) {
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- result_data
        if (!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
        if (!is.null(result_data$actual_value)) result$actual_value <- result_data$actual_value
      } else {
        result$error <- result_data$error
      }
      
    } else if (sport == "nhl" && market_info$type == "game_total_goals_oddeven") {
      result_data <- resolve_nhl_game_total_goals(game_id, line_value, NULL, TRUE)
      if (result_data$success) {
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- result_data
        if (!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
        if (!is.null(result_data$actual_value)) result$actual_value <- result_data$actual_value
      } else {
        result$error <- result_data$error
      }
      
    } else if (sport == "nhl" && market_info$type == "game_total_shots") {
      result_data <- resolve_nhl_game_total_shots(game_id, line_value, if(grepl("over", market_lower)) "over" else if(grepl("under", market_lower)) "under" else NULL)
      if (result_data$success) {
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- result_data
        if (!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
        if (!is.null(result_data$actual_value)) result$actual_value <- result_data$actual_value
      } else {
        result$error <- result_data$error
      }
      
    } else if (sport == "nhl" && market_info$type == "game_power_play_goals") {
      result_data <- resolve_nhl_game_power_play_goals(game_id, line_value, if(grepl("over", market_lower)) "over" else if(grepl("under", market_lower)) "under" else NULL)
      if (result_data$success) {
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- result_data
        if (!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
        if (!is.null(result_data$actual_value)) result$actual_value <- result_data$actual_value
      } else {
        result$error <- result_data$error
      }
      
      # ==================== PROMOTED PLAYER PERIOD STATS HANDLER (MUST BE FIRST) ====================
    } else if (market_info$type == "player_period_stat") {
      debug_cat("Processing player_period_stat market (High Priority)...\n")
      
      # NBA player period stats (quarters)
      if (sport == "nba" || sport == "basketball") {
        debug_cat(sprintf("  Resolving NBA player period stat: %s for %s in %s\n", 
                          market_info$stat, player_name, market_info$period))
        
        game_data <- fetch_game_data_for_resolution(game_id, sport)
        
        if (!is.null(game_data) && !is.null(game_data$plays) && is.data.frame(game_data$plays)) {
          
          # Determine period number (supports both "quarter_1" and "period_1" formats)
          if (grepl("quarter", market_info$period)) {
            period_num <- as.numeric(gsub("quarter_", "", market_info$period))
            period_type <- "quarter"
          } else if (grepl("period", market_info$period)) {
            period_num <- as.numeric(gsub("period_", "", market_info$period))
            period_type <- "period"
          } else {
            period_num <- 1
            period_type <- "unknown"
          }
          
          debug_cat(sprintf("  Looking at %s %d for player: %s\n", period_type, period_num, player_name))
          
          # Filter plays for the specific period - handle different column names
          if ("period.number" %in% names(game_data$plays)) {
            period_plays <- game_data$plays[game_data$plays$period.number == period_num, ]
          } else if ("period" %in% names(game_data$plays)) {
            period_plays <- game_data$plays[game_data$plays$period == period_num, ]
          } else {
            result$error <- "Cannot find period information in play data"
            next
          }
          
          debug_cat(sprintf("  Total plays in %s %d: %d\n", period_type, period_num, nrow(period_plays)))
          
          # Find plays involving the player (case-insensitive)
          player_plays <- period_plays[grepl(player_name, period_plays$text, ignore.case = TRUE), ]
          debug_cat(sprintf("  Plays involving %s: %d\n", player_name, nrow(player_plays)))
          
          actual_value <- 0
          
          if (market_info$stat == "points") {
            # Sum points from scoring plays WHERE THE PLAYER ACTUALLY SCORED (not assisted)
            for (i in 1:nrow(player_plays)) {
              play_text <- player_plays$text[i]
              # Check if the player actually scored (not just involved)
              if (player_plays$scoringPlay[i] == TRUE) {
                # Look for patterns where the player is the scorer
                # Pattern: "Player Name makes/made/hits" (the player is the one taking the shot)
                if (grepl(paste0(player_name, " makes|made|hits"), play_text, ignore.case = TRUE)) {
                  points <- player_plays$scoreValue[i] %||% 2
                  actual_value <- actual_value + points
                  debug_cat(sprintf("    Scoring play (by %s): %s (+%d points)\n", player_name, play_text, points))
                } else {
                  debug_cat(sprintf("    Skipping assist/other: %s\n", play_text))
                }
              }
            }
            debug_cat(sprintf("  Total %s in %s %d: %d\n", market_info$stat, period_type, period_num, actual_value))
            
          } else if (market_info$stat == "assists") {
            # Count assists - look for "Player Name assists" pattern
            for (i in 1:nrow(player_plays)) {
              play_text <- player_plays$text[i]
              if (grepl(paste0(player_name, " assists"), play_text, ignore.case = TRUE)) {
                actual_value <- actual_value + 1
                debug_cat(sprintf("    Assist by %s: %s\n", player_name, play_text))
              }
            }
            debug_cat(sprintf("  Total %s in %s %d: %d\n", market_info$stat, period_type, period_num, actual_value))
            
          } else if (market_info$stat == "rebounds") {
            # Count rebounds - look for "Player Name rebound" pattern
            for (i in 1:nrow(player_plays)) {
              play_text <- player_plays$text[i]
              if (grepl(paste0(player_name, ".*rebound"), play_text, ignore.case = TRUE)) {
                actual_value <- actual_value + 1
                debug_cat(sprintf("    Rebound by %s: %s\n", player_name, play_text))
              }
            }
            debug_cat(sprintf("  Total %s in %s %d: %d\n", market_info$stat, period_type, period_num, actual_value))
          }
          
          result$success <- TRUE
          result$resolved <- TRUE
          result$actual_value <- actual_value
          result$data <- list(
            period_type = period_type,
            period_number = period_num,
            stat = market_info$stat,
            value = actual_value,
            plays_found = nrow(player_plays)
          )
          
          # Check against line value if provided
          if (!is.null(line_value)) {
            line_val_num <- as.numeric(line_value)
            if (grepl("over", market_lower)) {
              result$bet_won <- actual_value > line_val_num
            } else if (grepl("under", market_lower)) {
              result$bet_won <- actual_value < line_val_num
            }
            result$line_value <- line_val_num
            debug_cat(sprintf("  Line: %.1f %s -> Bet won: %s\n", 
                              line_val_num, 
                              if(grepl("over", market_lower)) "over" else "under",
                              result$bet_won))
          }
          
        } else {
          result$error <- "Could not fetch play-by-play data for player period stats"
          debug_cat("  ERROR: No play-by-play data available\n")
        }
        
      } else if (sport == "nhl") {
        # NHL player period stats
        result_data <- resolve_nhl_player_period_market(game_id, player_name, market_info, market_lower, line_value)
        if (result_data$success) {
          result$success <- TRUE
          result$resolved <- TRUE
          result$data <- result_data
          if (!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
          if (!is.null(result_data$actual_value)) result$actual_value <- result_data$actual_value
        } else {
          result$error <- result_data$error
        }
      } else {
        result$error <- paste("Player period stats not supported for sport:", sport)
      }
      
      # ==================== PLAYER COMBO & SPECIAL ====================
    } else if (market_info$type == "player_combo") {
      debug_cat("Market type: Player combo prop\n")
      combo_result <- resolve_combo_prop(game_id, player_name, sport, result$season,
                                         market_info$combo_type, line_value)
      if (combo_result$success) {
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- combo_result
        if (!is.null(line_value)) {
          line_val_num <- as.numeric(line_value)
          if (grepl("over", market_lower)) result$bet_won <- (combo_result$actual_value > line_val_num)
          else if (grepl("under", market_lower)) result$bet_won <- (combo_result$actual_value < line_val_num)
          result$actual_value <- combo_result$actual_value
          result$line_value <- line_val_num
        }
      } else {
        result$error <- combo_result$error
      }
      
    } else if (market_info$type == "player_special") {
      debug_cat(sprintf("Market type: Special prop - %s\n", market_info$special_type))
      special_result <- resolve_special_prop(game_id, player_name, sport, result$season,
                                             market_info$special_type, line_value)
      if (special_result$success) {
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- special_result
        if (market_info$special_type %in% c("double_double", "triple_double")) {
          if (!is.null(special_result$achieved)) result$bet_won <- special_result$achieved
        } else if (market_info$special_type %in% c("three_pointers_made")) {
          if (!is.null(line_value) && !is.null(special_result$actual_value)) {
            line_val_num <- as.numeric(line_value)
            if (grepl("over", market_lower)) result$bet_won <- special_result$actual_value > line_val_num
            else if (grepl("under", market_lower)) result$bet_won <- special_result$actual_value < line_val_num
            result$actual_value <- special_result$actual_value
            result$line_value <- line_val_num
          }
        }
      } else {
        result$error <- special_result$error
      }
      
      # ==================== PLAYER STAT MARKETS (MOVED UP - BEFORE INNING MARKETS) ====================
    } else if (market_info$type == "player_stat") {
      debug_cat(sprintf("Market type: Player stat - %s\n", market_info$stat))
      
      if (market_info$stat == "total_bases") {
        if (sport %in% c("mlb", "baseball")) {
          stats_data <- fetch_mlb_player_stats(player_name, result$season, game_date_obj, game_id)
          if (!is.null(stats_data) && stats_data$found) {
            result$success <- TRUE
            result$resolved <- TRUE
            result$data <- stats_data
            actual_value <- get_correct_player_stat(stats_data, "total_bases", sport)
            if (!is.null(actual_value) && !is.na(actual_value)) {
              result$actual_value <- actual_value
              if (!is.null(line_value)) {
                line_val_num <- as.numeric(line_value)
                if (grepl("over", market_lower)) result$bet_won <- actual_value > line_val_num
                else if (grepl("under", market_lower)) result$bet_won <- actual_value < line_val_num
                result$line_value <- line_val_num
              }
            }
          } else {
            result$error <- stats_data$error %||% "Failed to fetch player stats"
          }
        } else {
          result$error <- "Total bases only available for MLB"
        }
      } else if (market_info$stat == "three_pointers_made") {
        special_result <- resolve_special_prop(game_id, player_name, sport, result$season,
                                               "three_pointers_made", line_value)
        if (special_result$success) {
          result$success <- TRUE
          result$resolved <- TRUE
          result$data <- special_result
          if (!is.null(line_value) && !is.null(special_result$actual_value)) {
            line_val_num <- as.numeric(line_value)
            if (grepl("over", market_lower)) result$bet_won <- special_result$actual_value > line_val_num
            else if (grepl("under", market_lower)) result$bet_won <- special_result$actual_value < line_val_num
            result$actual_value <- special_result$actual_value
            result$line_value <- line_val_num
          }
        } else {
          result$error <- special_result$error
        }
      } else {
        if (sport %in% c("nba", "basketball")) {
          stats_data <- fetch_nba_player_stats(player_name, result$season, game_date_obj, game_id)
        } else if (sport %in% c("nfl", "football")) {
          stats_data <- fetch_nfl_player_stats(player_name, result$season, game_id = game_id)
        } else if (sport %in% c("mlb")) {
          stats_data <- fetch_mlb_player_stats(player_name, result$season, game_date_obj, game_id)
        } else if (sport %in% c("wnba")) {
          stats_data <- fetch_wnba_player_stats(player_name, result$season, game_date_obj, game_id)
        } else if (sport %in% c("ncaab", "ncaaw")) {
          stats_data <- fetch_ncaa_bball_player_stats(player_name, result$season, game_date_obj, game_id, sport)
        } else {
          stats_data <- NULL
        }
        
        if (!is.null(stats_data) && stats_data$found) {
          result$success <- TRUE
          result$resolved <- TRUE
          result$data <- stats_data
          actual_value <- get_correct_player_stat(stats_data, market_info$stat, sport)
          debug_cat(sprintf("  Stat lookup: requested='%s', got=%s (sport=%s)\n",
                            market_info$stat, ifelse(is.na(actual_value), "NA", as.character(actual_value)), sport))
          if (!is.null(actual_value) && !is.na(actual_value)) {
            result$actual_value <- actual_value
            if (!is.null(line_value)) {
              line_val_num <- as.numeric(line_value)
              if (grepl("over", market_lower)) result$bet_won <- actual_value > line_val_num
              else if (grepl("under", market_lower)) result$bet_won <- actual_value < line_val_num
              result$line_value <- line_val_num
            }
          }
        } else {
          error_msg <- if (!is.null(stats_data)) stats_data$error else "Failed to fetch player stats"
          result$error <- error_msg
        }
      }
      
    } else if (market_info$type == "team_score_first") {
      debug_cat("Market type: Team to score first\n")
      
      # Handle NFL separately using nflreadr
      if (sport == "nfl" || sport == "football") {
        debug_cat("  Using nflreadr for NFL game data\n")
        
        # Load play-by-play data for the season
        pbp <- nflreadr::load_pbp(result$season)
        
        if (!is.null(pbp) && nrow(pbp) > 0) {
          # Filter for the specific game
          game_plays <- pbp[pbp$game_id == game_id, ]
          
          if (nrow(game_plays) > 0) {
            # Find first scoring play (touchdown or field goal)
            scoring_plays <- game_plays %>%
              filter(!is.na(td_team) | (!is.na(field_goal_result) & field_goal_result == "made")) %>%
              arrange(play_id)
            
            if (nrow(scoring_plays) > 0) {
              first_play <- scoring_plays[1, ]
              
              # Determine which team scored
              scoring_team <- first_play$td_team %||% first_play$posteam
              
              # Get team names
              home_team_std <- standardize_team_name(teams$home, "nfl")
              away_team_std <- standardize_team_name(teams$away, "nfl")
              
              # Determine if our bet team scored first
              bet_team_std <- standardize_team_name(player_name, "nfl")
              result$bet_won <- FALSE
              
              if (!is.na(scoring_team) && !is.na(bet_team_std)) {
                result$bet_won <- (scoring_team == bet_team_std)
                result$first_scorer <- if(scoring_team == home_team_std) "home" else "away"
              }
              
              result$success <- TRUE
              result$resolved <- TRUE
              result$actual_value <- result$first_scorer
              result$data <- list(
                first_scorer = result$first_scorer,
                scoring_team = scoring_team,
                play_description = first_play$desc,
                quarter = first_play$qtr,
                time = first_play$game_seconds_remaining
              )
              
              debug_cat(sprintf("  First scoring team: %s\n", scoring_team))
              debug_cat(sprintf("  Bet team (%s) scored first: %s\n", player_name, result$bet_won))
              
            } else {
              result$error <- "No scoring plays found in game"
            }
          } else {
            result$error <- "Game not found in play-by-play data"
          }
        } else {
          result$error <- "Could not load NFL play-by-play data"
        }
        
      } else {
        # For other sports (NBA, NHL, MLB, etc.) use ESPN API
        debug_cat("  Using ESPN API for non-NFL sport\n")
        
        # Fetch game data
        game_data <- fetch_game_data_for_resolution(game_id, sport)
        
        if (!is.null(game_data) && !is.null(game_data$plays) && is.data.frame(game_data$plays)) {
          # Find first scoring play of the game
          scoring_plays <- game_data$plays[game_data$plays$scoringPlay == TRUE, ]
          
          if (nrow(scoring_plays) > 0) {
            first_play <- scoring_plays[1, ]
            play_text <- tolower(first_play$text)
            
            # Determine which team scored
            home_team_lower <- tolower(teams$home)
            away_team_lower <- tolower(teams$away)
            
            # Extract team names without location (e.g., "Bears" from "Chicago Bears")
            home_short <- gsub(".*?([A-Z][a-z]+)$", "\\1", teams$home)
            away_short <- gsub(".*?([A-Z][a-z]+)$", "\\1", teams$away)
            
            result$first_scorer <- "unknown"
            if (grepl(home_team_lower, play_text) || grepl(tolower(home_short), play_text)) {
              result$first_scorer <- "home"
            } else if (grepl(away_team_lower, play_text) || grepl(tolower(away_short), play_text)) {
              result$first_scorer <- "away"
            }
            
            # Determine if our bet team scored first
            bet_team_lower <- tolower(player_name)
            result$bet_won <- FALSE
            
            if (result$first_scorer == "home" && (grepl(bet_team_lower, home_team_lower) || grepl(bet_team_lower, tolower(home_short)))) {
              result$bet_won <- TRUE
            } else if (result$first_scorer == "away" && (grepl(bet_team_lower, away_team_lower) || grepl(bet_team_lower, tolower(away_short)))) {
              result$bet_won <- TRUE
            }
            
            result$success <- TRUE
            result$resolved <- TRUE
            result$actual_value <- result$first_scorer
            result$data <- list(
              first_scorer = result$first_scorer,
              play_description = first_play$text,
              play_time = first_play$clock.displayValue %||% "Unknown",
              period = first_play$period.number %||% 1
            )
            
            debug_cat(sprintf("  First scorer: %s\n", result$first_scorer))
            debug_cat(sprintf("  Bet team (%s) scored first: %s\n", player_name, result$bet_won))
            
          } else {
            result$error <- "No scoring plays found in game"
          }
        } else {
          result$error <- "Could not fetch play-by-play data for team score first"
        }
      }
      
      # ==================== PERIOD/QUARTER/INNING MARKETS ====================
    } else if (market_info$type %in% c("period_moneyline", "period_moneyline_3way", "period_spread", 
                                       "period_total", "period_total_oddeven", "period_both_teams_score")) {
      
      if (market_info$type %in% c("period_moneyline", "period_moneyline_3way")) {
        period <- market_info$period
        bet_team <- if (!is.null(teams$team_only) && teams$team_only) teams$home else NULL
        result_data <- resolve_period_moneyline(game_id, sport, period, bet_team, 
                                                market_info$type == "period_moneyline_3way")
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- result_data
        if (!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
        
      } else if (market_info$type == "period_spread") {
        bet_team <- if (!is.null(teams$team_only) && teams$team_only) teams$home else "home"
        result_data <- resolve_period_spread(game_id, sport, market_info$period, line_value, bet_team)
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- result_data
        if (!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
        
      } else if (market_info$type %in% c("period_total", "period_total_oddeven")) {
        direction <- if (grepl("over", market_lower)) "over" else if (grepl("under", market_lower)) "under" else 
          if (grepl("odd", market_lower)) "odd" else if (grepl("even", market_lower)) "even" else "over"
        result_data <- resolve_period_total(game_id, sport, result$season, market_info$period, line_value, direction)
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- result_data
        if (!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
        
      } else if (market_info$type == "period_both_teams_score") {
        result_data <- resolve_period_moneyline(game_id, sport, market_info$period)
        result$both_scored <- (result_data$home_score > 0 && result_data$away_score > 0)
        result$bet_won <- result$both_scored
        result$success <- TRUE
        result$resolved <- TRUE
      }
      
      # ==================== INNING MARKETS ====================
    } else if (market_info$type %in% c("inning_moneyline", "inning_moneyline_3way")) {
      result_data <- resolve_inning_moneyline(game_id, market_info$inning, NULL,
                                              market_info$type == "inning_moneyline_3way")
      result$success <- TRUE
      result$resolved <- TRUE
      result$data <- result_data
      if (!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
      
    } else if (market_info$type == "inning_total_runs") {
      direction <- if (grepl("over", market_lower)) "over" else "under"
      result_data <- resolve_inning_total_runs(game_id, market_info$inning, line_value, direction)
      result$success <- TRUE
      result$resolved <- TRUE
      result$data <- result_data
      if (!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
      
    } else if (grepl("multi_inning", market_info$type)) {
      market_subtype <- gsub("multi_inning_", "", market_info$type)
      direction <- if (grepl("over", market_lower)) "over" else if (grepl("under", market_lower)) "under" else NULL
      
      debug_cat(sprintf("  Resolving multi-inning market: %d innings, subtype: %s\n", 
                        market_info$innings, market_subtype))
      
      # Fetch game data for multi-inning resolution
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      
      if (market_subtype == "total_runs") {
        result_data <- resolve_multi_inning_total_runs(game_data, market_info$innings, line_value, direction)
      } else {
        result_data <- resolve_multi_inning_market(game_id, market_info$innings, market_subtype, 
                                                   line_value, direction, NULL)
      }
      
      if (result_data$success) {
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- result_data
        if (!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
        if (!is.null(result_data$actual_value)) result$actual_value <- result_data$actual_value
        debug_cat(sprintf("  Multi-inning resolution successful. Total: %d, Bet won: %s\n", 
                          result_data$actual_value, result_data$bet_won))
      } else {
        result$error <- result_data$error %||% "Multi-inning resolution failed"
        debug_cat(sprintf("  ERROR: %s\n", result$error))
      }
      
      # ==================== MLB INNING MARKERS (pattern matching) ====================
    } else if (grepl("^[1-9][0-9]?st|nd|rd|th Inning Total Runs$", market_lower, ignore.case = TRUE)) {
      inning_num <- as.numeric(gsub("[^0-9]", "", market_lower))
      direction <- if(grepl("over", market_lower)) "over" else if(grepl("under", market_lower)) "under" else NULL
      
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        result_data <- resolve_inning_total_runs(game_data, inning_num, line_value, direction)
        result$success <- result_data$success
        result$resolved <- result_data$success
        result$data <- result_data
        if(!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
        if(!is.null(result_data$actual_value)) result$actual_value <- result_data$actual_value
      } else {
        result$error <- "Could not fetch game data for inning resolution"
      }
      
    } else if (grepl("1st [3-9] Innings Total Runs", market_lower, ignore.case = TRUE)) {
      num_innings <- as.numeric(gsub("[^0-9]", "", market_lower))
      direction <- if(grepl("over", market_lower)) "over" else if(grepl("under", market_lower)) "under" else NULL
      
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        result_data <- resolve_multi_inning_total_runs(game_data, num_innings, line_value, direction)
        result$success <- result_data$success
        result$resolved <- result_data$success
        result$data <- result_data
        if(!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
      } else {
        result$error <- "Could not fetch game data for multi-inning resolution"
      }
      
      # ==================== MLB SPECIAL MARKETS ====================
    } else if (market_info$type == "largest_comeback") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        result_data <- resolve_largest_comeback(game_data)
        result$success <- result_data$success
        result$resolved <- result_data$success
        result$data <- result_data
      } else {
        result$error <- "Could not fetch game data for comeback resolution"
      }
      
    } else if (market_info$type == "team_score_first_and_win") {
      team_name <- if(!is.null(teams$team_only) && teams$team_only) teams$home else player_name
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        result_data <- resolve_team_score_first_and_win(game_data, team_name)
        result$success <- result_data$success
        result$resolved <- result_data$success
        result$data <- result_data
        if(!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
      } else {
        result$error <- "Could not fetch game data for team score first resolution"
      }
      
    } else if (market_info$type == "game_both_teams_score") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        result_data <- resolve_both_teams_score(game_data)
        result$success <- result_data$success
        result$resolved <- result_data$success
        result$data <- result_data
        if(!is.null(result_data$both_scored)) result$bet_won <- result_data$both_scored
      } else {
        result$error <- "Could not fetch game data for both teams score resolution"
      }
      
    } else if (market_info$type == "game_home_bat_bottom_9th") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data) && !is.null(game_data$plays)) {
        inning_9_plays <- game_data$plays[game_data$plays$period == 9, ]
        if(nrow(inning_9_plays) > 0) {
          top_9_plays <- inning_9_plays[grepl("Top", inning_9_plays$period.displayValue, ignore.case = TRUE), ]
          if(nrow(top_9_plays) > 0) {
            last_top_play <- top_9_plays[nrow(top_9_plays), ]
            away_score <- last_top_play$awayScore
            home_score <- last_top_play$homeScore
            result$success <- TRUE
            result$resolved <- TRUE
            result$bet_won <- (!is.null(away_score) && !is.null(home_score) && away_score > home_score)
            result$data <- list(away_score = away_score, home_score = home_score, home_bats = away_score > home_score)
          } else {
            result$error <- "Could not find top of 9th inning data"
          }
        } else {
          result$error <- "Game did not go 9 innings"
        }
      } else {
        result$error <- "Could not fetch game data"
      }
      
    } else if (market_info$type == "total_grand_slams") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data) && !is.null(game_data$plays)) {
        scoring_plays <- game_data$plays[!is.na(game_data$plays$scoringPlay) & game_data$plays$scoringPlay == TRUE, ]
        grand_slams <- 0
        for(i in 1:nrow(scoring_plays)) {
          play_text <- tolower(scoring_plays$text[i])
          if(grepl("grand slam", play_text) || (grepl("homer", play_text) && grepl("bases loaded", play_text))) {
            grand_slams <- grand_slams + 1
          }
        }
        result$success <- TRUE
        result$resolved <- TRUE
        result$actual_value <- grand_slams
        result$data <- list(grand_slams = grand_slams)
      } else {
        result$error <- "Could not fetch game data"
      }
      
    } else if (market_info$type == "team_most_hits") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data) && !is.null(game_data$boxscore$teams)) {
        away_hits <- as.numeric(game_data$boxscore$teams$statistics[[1]]$hits[1] %||% 0)
        home_hits <- as.numeric(game_data$boxscore$teams$statistics[[2]]$hits[1] %||% 0)
        
        bet_team <- if(!is.null(teams$team_only) && teams$team_only) "home" else 
          if(grepl(teams$home, player_name, ignore.case=TRUE)) "home" else "away"
        
        result$success <- TRUE
        result$resolved <- TRUE
        if(away_hits > home_hits) {
          result$winner <- "away"
          result$bet_won <- (bet_team == "away")
        } else if(home_hits > away_hits) {
          result$winner <- "home"
          result$bet_won <- (bet_team == "home")
        } else {
          result$winner <- "tie"
          result$bet_won <- FALSE
        }
        result$data <- list(away_hits = away_hits, home_hits = home_hits)
      } else {
        result$error <- "Could not fetch hit data"
      }
      
    } else if (market_info$type == "team_most_innings") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data) && !is.null(game_data$plays)) {
        inning_runs <- get_inning_runs(game_data$plays, teams$away, teams$home)
        if(!is.null(inning_runs)) {
          away_wins <- sum(inning_runs$away_runs > inning_runs$home_runs, na.rm = TRUE)
          home_wins <- sum(inning_runs$home_runs > inning_runs$away_runs, na.rm = TRUE)
          
          bet_team <- if(!is.null(teams$team_only) && teams$team_only) "home" else 
            if(grepl(teams$home, player_name, ignore.case=TRUE)) "home" else "away"
          
          result$success <- TRUE
          result$resolved <- TRUE
          if(away_wins > home_wins) {
            result$winner <- "away"
            result$bet_won <- (bet_team == "away")
          } else if(home_wins > away_wins) {
            result$winner <- "home"
            result$bet_won <- (bet_team == "home")
          } else {
            result$winner <- "tie"
            result$bet_won <- FALSE
          }
          result$data <- list(away_wins = away_wins, home_wins = home_wins)
        } else {
          result$error <- "Could not calculate inning wins"
        }
      } else {
        result$error <- "Could not fetch game data"
      }
      
    } else if (market_info$type == "team_highest_scoring_inning") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data) && !is.null(game_data$plays)) {
        inning_runs <- get_inning_runs(game_data$plays, teams$away, teams$home)
        if(!is.null(inning_runs)) {
          max_away <- max(inning_runs$away_runs, na.rm = TRUE)
          max_home <- max(inning_runs$home_runs, na.rm = TRUE)
          
          bet_team <- if(!is.null(teams$team_only) && teams$team_only) "home" else 
            if(grepl(teams$home, player_name, ignore.case=TRUE)) "home" else "away"
          
          result$success <- TRUE
          result$resolved <- TRUE
          if(max_away > max_home) {
            result$winner <- "away"
            result$bet_won <- (bet_team == "away")
          } else if(max_home > max_away) {
            result$winner <- "home"
            result$bet_won <- (bet_team == "home")
          } else {
            result$winner <- "tie"
            result$bet_won <- FALSE
          }
          result$data <- list(max_away = max_away, max_home = max_home)
        } else {
          result$error <- "Could not calculate inning runs"
        }
      } else {
        result$error <- "Could not fetch game data"
      }
      
      # ==================== NBA QUARTER/HALF MARKETS ====================
    } else if (grepl("^[1-4]st|nd|rd|th Quarter Total Points$", market_lower, ignore.case = TRUE) && sport == "nba") {
      quarter_num <- as.numeric(gsub("[^0-9]", "", market_lower))
      direction <- if(grepl("over", market_lower)) "over" else if(grepl("under", market_lower)) "under" else NULL
      
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        result_data <- resolve_nba_quarter_total(game_data, quarter_num, line_value, direction)
        result$success <- result_data$success
        result$resolved <- result_data$success
        result$data <- result_data
        if(!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
      } else {
        result$error <- "Could not fetch game data for NBA quarter resolution"
      }
      
    } else if (grepl("^[1-2]st|nd Half Total Points$", market_lower, ignore.case = TRUE) && sport == "nba") {
      half_num <- as.numeric(gsub("[^0-9]", "", market_lower))
      direction <- if(grepl("over", market_lower)) "over" else if(grepl("under", market_lower)) "under" else NULL
      
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        result_data <- resolve_nba_half_total(game_data, half_num, line_value, direction)
        result$success <- result_data$success
        result$resolved <- result_data$success
        result$data <- result_data
        if(!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
      } else {
        result$error <- "Could not fetch game data for NBA half resolution"
      }
      
    } else if (grepl("team win both halves", market_lower, ignore.case = TRUE)) {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        quarters <- get_nba_quarter_scores(game_data)
        if(!is.null(quarters) && length(quarters) >= 4) {
          first_half_away <- quarters[[1]]$away_score + quarters[[2]]$away_score
          first_half_home <- quarters[[1]]$home_score + quarters[[2]]$home_score
          second_half_away <- quarters[[3]]$away_score + quarters[[4]]$away_score
          second_half_home <- quarters[[3]]$home_score + quarters[[4]]$home_score
          
          first_half_winner <- if(first_half_away > first_half_home) "away" else "home"
          second_half_winner <- if(second_half_away > second_half_home) "away" else "home"
          
          bet_team <- if(!is.null(teams$team_only) && teams$team_only) "home" else NULL
          
          result$success <- TRUE
          result$resolved <- TRUE
          result$first_half_winner <- first_half_winner
          result$second_half_winner <- second_half_winner
          result$bet_won <- (!is.null(bet_team) && first_half_winner == bet_team && second_half_winner == bet_team)
        } else {
          result$error <- "Could not extract quarter data"
        }
      } else {
        result$error <- "Could not fetch game data for team win both halves resolution"
      }
      
    } else if (market_info$type == "team_win_every_quarter") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        quarters <- get_nba_quarter_scores(game_data)
        if(!is.null(quarters) && length(quarters) >= 4) {
          bet_team <- if(!is.null(teams$team_only) && teams$team_only) "home" else 
            if(grepl(teams$home, player_name, ignore.case=TRUE)) "home" else "away"
          won_all <- TRUE
          for(q in 1:4) {
            if(quarters[[q]]$away_score > quarters[[q]]$home_score) {
              if(bet_team != "away") won_all <- FALSE
            } else if(quarters[[q]]$home_score > quarters[[q]]$away_score) {
              if(bet_team != "home") won_all <- FALSE
            } else {
              won_all <- FALSE
            }
          }
          result$success <- TRUE
          result$resolved <- TRUE
          result$bet_won <- won_all
          result$data <- list(won_all_quarters = won_all)
        } else {
          result$error <- "Could not extract quarter data"
        }
      } else {
        result$error <- "Could not fetch game data"
      }
      
      # ==================== NHL PERIOD MARKETS ====================
    } else if (grepl("^[1-3]st|nd|rd Period Total Goals$", market_lower, ignore.case = TRUE) && sport == "nhl") {
      period_num <- as.numeric(gsub("[^0-9]", "", market_lower))
      direction <- if(grepl("over", market_lower)) "over" else if(grepl("under", market_lower)) "under" else NULL
      
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        result_data <- resolve_nhl_period_total(game_data, period_num, line_value, direction)
        result$success <- result_data$success
        result$resolved <- result_data$success
        result$data <- result_data
        if(!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
      } else {
        result$error <- "Could not fetch game data for NHL period resolution"
      }
      
    } else if (market_info$type == "goal_each_period") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data) && !is.null(game_data$boxscore$periods)) {
        periods <- game_data$boxscore$periods
        goals_each_period <- TRUE
        for(i in 1:min(3, nrow(periods))) {
          if(as.numeric(periods$awayScore[i]) == 0 && as.numeric(periods$homeScore[i]) == 0) {
            goals_each_period <- FALSE
            break
          }
        }
        result$success <- TRUE
        result$resolved <- TRUE
        result$bet_won <- goals_each_period
        result$actual_value <- goals_each_period
        result$data <- list(goals_in_each_period = goals_each_period, period_scores = periods)
      } else {
        result$error <- "Could not fetch period data"
      }
      
    } else if (market_info$type == "highest_scoring_period") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data) && !is.null(game_data$boxscore$periods)) {
        periods <- game_data$boxscore$periods
        period_totals <- as.numeric(periods$awayScore) + as.numeric(periods$homeScore)
        max_total <- max(period_totals[1:3], na.rm = TRUE)
        highest_period <- which.max(period_totals[1:3])
        
        result$success <- TRUE
        result$resolved <- TRUE
        result$actual_value <- highest_period
        result$data <- list(
          highest_period = highest_period, 
          period_totals = period_totals[1:3],
          period_1_total = period_totals[1],
          period_2_total = period_totals[2],
          period_3_total = period_totals[3]
        )
        
        if(!is.null(line_value)) {
          bet_period <- as.numeric(line_value)
          result$bet_won <- (highest_period == bet_period)
        }
      } else {
        result$error <- "Could not fetch period data"
      }
      
      # ==================== ADDITIONAL MARKET HANDLERS ====================
    } else if (market_info$stat == "batting_fantasy_score") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        result_data <- resolve_batting_fantasy_score(game_data, player_name)
        result$success <- result_data$success
        result$resolved <- result_data$success
        result$actual_value <- result_data$actual_value
        result$data <- result_data
        if(!is.null(line_value)) {
          line_val_num <- as.numeric(line_value)
          if(grepl("over", market_lower)) result$bet_won <- result_data$actual_value > line_val_num
          else if(grepl("under", market_lower)) result$bet_won <- result_data$actual_value < line_val_num
        }
      } else {
        result$error <- "Could not fetch game data"
      }
      
    } else if (market_info$stat == "dunks") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        result_data <- resolve_player_dunks(game_data, player_name)
        result$success <- result_data$success
        result$resolved <- result_data$success
        result$actual_value <- result_data$actual_value
        result$data <- result_data
        if(!is.null(line_value)) {
          line_val_num <- as.numeric(line_value)
          if(grepl("over", market_lower)) result$bet_won <- result_data$actual_value > line_val_num
          else if(grepl("under", market_lower)) result$bet_won <- result_data$actual_value < line_val_num
        }
      } else {
        result$error <- "Could not fetch game data"
      }
      
    } else if (market_info$stat == "hits_runs_rbi") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        result_data <- resolve_player_hits_runs_rbi(game_data, player_name)
        result$success <- result_data$success
        result$resolved <- result_data$success
        result$actual_value <- result_data$actual_value
        result$data <- result_data
        if(!is.null(line_value)) {
          line_val_num <- as.numeric(line_value)
          if(grepl("over", market_lower)) result$bet_won <- result_data$actual_value > line_val_num
          else if(grepl("under", market_lower)) result$bet_won <- result_data$actual_value < line_val_num
        }
      } else {
        result$error <- "Could not fetch game data"
      }
      
    } else if (market_info$type == "team_total" && market_info$stat == "batting_walks") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        innings <- if(!is.null(market_info$innings)) market_info$innings else 5
        result_data <- resolve_team_inning_batting_walks(game_data, teams$home, innings)
        result$success <- result_data$success
        result$resolved <- result_data$success
        result$actual_value <- result_data$actual_value
        result$data <- result_data
      } else {
        result$error <- "Could not fetch game data"
      }
      
    } else if (market_info$type == "team_total" && market_info$stat == "pitching_strikeouts") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        innings <- if(!is.null(market_info$innings)) market_info$innings else 5
        result_data <- resolve_team_inning_pitching_strikeouts(game_data, teams$home, innings)
        result$success <- result_data$success
        result$resolved <- result_data$success
        result$actual_value <- result_data$actual_value
        result$data <- result_data
      } else {
        result$error <- "Could not fetch game data"
      }
      
    } else if (market_info$type == "team_total" && market_info$stat == "batters") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        result_data <- resolve_team_inning_batters(game_data, teams$home)
        result$success <- result_data$success
        result$resolved <- result_data$success
        result$actual_value <- result_data$actual_value
        result$data <- result_data
      } else {
        result$error <- "Could not fetch game data"
      }
      
    } else if (market_info$type == "team_total" && market_info$stat == "pitches_thrown") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        result_data <- resolve_team_inning_pitches(game_data, teams$home)
        result$success <- result_data$success
        result$resolved <- result_data$success
        result$actual_value <- result_data$actual_value
        result$data <- result_data
      } else {
        result$error <- "Could not fetch game data"
      }
      
    } else if (market_info$type == "team_total" && market_info$stat == "strikeouts_thrown") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        result_data <- resolve_team_inning_strikeouts_thrown(game_data, teams$home)
        result$success <- result_data$success
        result$resolved <- result_data$success
        result$actual_value <- result_data$actual_value
        result$data <- result_data
      } else {
        result$error <- "Could not fetch game data"
      }
      
    } else if (market_info$type == "team_player_first") {
      game_data <- fetch_game_data_for_resolution(game_id, sport)
      if(!is.null(game_data)) {
        team_name <- teams$home
        result_data <- resolve_team_player_first_basket(game_data, team_name)
        result$success <- result_data$success
        result$resolved <- result_data$success
        if(!is.null(result_data$player)) {
          result$actual_value <- result_data$player
          result$bet_won <- grepl(player_name, result_data$player, ignore.case = TRUE)
        }
        result$data <- result_data
      } else {
        result$error <- "Could not fetch game data"
      }
      
      # ==================== SPECIAL GAME MARKETS ====================
    } else if (market_info$type == "game_overtime") {
      result_data <- resolve_game_overtime(game_id, sport)
      result$success <- TRUE
      result$resolved <- TRUE
      result$bet_won <- result_data$bet_won
      result$data <- result_data
      
    } else if (market_info$type == "game_extra_innings") {
      result_data <- resolve_game_extra_innings(game_id)
      result$success <- TRUE
      result$resolved <- TRUE
      result$bet_won <- result_data$bet_won
      result$data <- result_data
      
      # ==================== NHL MARKET HANDLING ====================
    } else if (sport == "nhl") {
      debug_cat("Processing NHL market...\n")
      
      if (market_info$type == "player_period_stat") {
        result_data <- resolve_nhl_player_period_market(game_id, player_name, market_info, market_lower, line_value)
        if (result_data$success) {
          result$success <- TRUE
          result$resolved <- TRUE
          result$data <- result_data
          if (!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
          if (!is.null(result_data$actual_value)) result$actual_value <- result_data$actual_value
        } else {
          result$error <- result_data$error
        }
      } else if (market_info$type %in% c("period_total_goals", "period_both_teams_score", "period_moneyline", "period_puckline")) {
        result_data <- resolve_nhl_period_market(game_id, market_info, market_lower, line_value)
        if (result_data$success) {
          result$success <- TRUE
          result$resolved <- TRUE
          result$data <- result_data
          if (!is.null(result_data$bet_won)) result$bet_won <- result_data$bet_won
        } else {
          result$error <- result_data$error
        }
      } else if (market_info$type == "player_stat") {
        stats_data <- get_espn_nhl_player_stats(game_id, player_name)
        if (!is.null(stats_data)) {
          result$success <- TRUE
          result$resolved <- TRUE
          result$data <- stats_data
          stat_value <- stats_data[[market_info$stat]]
          if (!is.null(stat_value)) {
            result$actual_value <- stat_value
            if (!is.null(line_value)) {
              line_val_num <- as.numeric(line_value)
              if (grepl("over", market_lower)) result$bet_won <- stat_value > line_val_num
              else if (grepl("under", market_lower)) result$bet_won <- stat_value < line_val_num
              result$line_value <- line_val_num
            }
          }
        } else {
          result$error <- "Could not fetch NHL player stats"
        }
      } else if (market_info$type %in% c("first_scorer", "last_scorer")) {
        debug_cat(sprintf("Resolving NHL %s scorer\n", market_info$type))
        scorer_result <- resolve_nhl_first_last_scorer(
          game_id, 
          player_name, 
          is_first = (market_info$type == "first_scorer")
        )
        
        if (scorer_result$success) {
          result$success <- TRUE
          result$resolved <- TRUE
          result$data <- scorer_result
          result$bet_won <- scorer_result$player_scored
          if (!is.null(scorer_result$scorer)) {
            result$actual_value <- scorer_result$scorer
          }
          debug_cat(sprintf("  Bet won: %s\n", scorer_result$player_scored))
        } else {
          result$error <- scorer_result$error
          debug_cat(sprintf("  ERROR: %s\n", scorer_result$error))
        }
      } else if (market_info$type %in% c("game_moneyline", "game_total_goals", "game_both_teams_score", "game_puckline")) {
        period_data <- get_espn_nhl_period_data(game_id)
        if (period_data$success) {
          result$success <- TRUE
          result$resolved <- TRUE
          result$data <- period_data
          if (market_info$type == "game_moneyline") {
            home_score <- as.numeric(period_data$game_data$home_score)
            away_score <- as.numeric(period_data$game_data$away_score)
            if (home_score > away_score) result$winner <- "home"
            else if (away_score > home_score) result$winner <- "away"
            else result$winner <- "tie"
          } else if (market_info$type == "game_total_goals") {
            total_goals <- as.numeric(period_data$game_data$home_score) + as.numeric(period_data$game_data$away_score)
            if (!is.null(line_value)) {
              line_val_num <- as.numeric(line_value)
              if (grepl("over", market_lower)) result$bet_won <- total_goals > line_val_num
              else if (grepl("under", market_lower)) result$bet_won <- total_goals < line_val_num
            }
          } else if (market_info$type == "game_both_teams_score") {
            both_scored <- (as.numeric(period_data$game_data$home_score) > 0 && as.numeric(period_data$game_data$away_score) > 0)
            result$bet_won <- both_scored
          }
        } else {
          result$error <- period_data$error
        }
      } else if (grepl("^regulation_", market_info$type)) {
        period_data <- get_espn_nhl_period_data(game_id)
        if (period_data$success) {
          result$success <- TRUE
          result$resolved <- TRUE
          reg_home_score <- 0
          reg_away_score <- 0
          num_periods <- length(period_data$periods)
          if (num_periods > 0) {
            for (p in 1:min(3, num_periods)) {
              reg_home_score <- reg_home_score + (period_data$periods[[p]]$home_score %||% 0)
              reg_away_score <- reg_away_score + (period_data$periods[[p]]$away_score %||% 0)
            }
          }
          result$regulation_total <- reg_home_score + reg_away_score
          if (market_info$type == "regulation_moneyline") {
            if (reg_home_score > reg_away_score) result$winner <- "home"
            else if (reg_away_score > reg_home_score) result$winner <- "away"
            else result$winner <- "tie"
          } else if (market_info$type == "regulation_total_goals" && !is.null(line_value)) {
            line_val_num <- as.numeric(line_value)
            if (grepl("over", market_lower)) result$bet_won <- result$regulation_total > line_val_num
            else if (grepl("under", market_lower)) result$bet_won <- result$regulation_total < line_val_num
          }
        }
      } else if (market_info$type == "unknown") {
        result$error <- paste("Unknown NHL market type:", market_type)
      }
      
    } else if (market_info$type == "period_total") {
      debug_cat(sprintf("Market type: %s total\n", market_info$period))
      period_result <- resolve_period_total(game_id, sport, result$season,
                                            market_info$period, line_value)
      if (period_result$success) {
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- period_result
        if (!is.null(line_value)) {
          if (grepl("over", market_lower)) result$bet_won <- period_result$total > as.numeric(line_value)
          else if (grepl("under", market_lower)) result$bet_won <- period_result$total < as.numeric(line_value)
          result$actual_value <- period_result$total
          result$line_value <- as.numeric(line_value)
        }
      } else {
        result$error <- period_result$error
      }
      
    } else if (market_info$type %in% c("first_scorer", "last_scorer")) {
      debug_cat(sprintf("Market type: %s scorer\n", market_info$type))
      if (market_info$type == "first_scorer") {
        scorer_data <- fetch_first_scorer(game_id, sport, result$season)
      } else {
        scorer_data <- fetch_last_scorer(game_id, sport, result$season)
      }
      if (scorer_data$found) {
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- scorer_data
        result$player_scored <- match_player_for_scorer(player_name, scorer_data$scorer)
        debug_cat(sprintf("Scorer: %s, Our player scored: %s\n", scorer_data$scorer, result$player_scored))
      } else {
        result$error <- scorer_data$error
      }
      
    } else if (grepl("total.*points|game.*total|points.*total", market_lower) && grepl("over|under", market_lower)) {
      debug_cat("Market type: Game total points\n")
      game_data <- fetch_game_result(game_id, sport, result$season)
      if (game_data$found) {
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- game_data
        line_val_num <- as.numeric(line_value)
        if (grepl("over", market_lower)) result$bet_won <- game_data$total_points > line_val_num
        else if (grepl("under", market_lower)) result$bet_won <- game_data$total_points < line_val_num
        result$actual_value <- game_data$total_points
        result$line_value <- line_val_num
      } else {
        result$error <- game_data$error
      }
      
    } else if (grepl("first.*scorer|first.*td|player.*first.*touchdown|first.*field.*goal|first.*basket", market_lower)) {
      debug_cat("Market type: First scorer\n")
      scorer_data <- fetch_first_scorer(game_id, sport, result$season)
      if (scorer_data$found) {
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- scorer_data
        result$player_scored <- match_player_for_scorer(player_name, scorer_data$scorer)
      } else {
        result$error <- scorer_data$error
      }
      
    } else if (grepl("last.*scorer|last.*td|player.*last.*touchdown", market_lower)) {
      debug_cat("Market type: Last scorer\n")
      scorer_data <- fetch_last_scorer(game_id, sport, result$season)
      if (scorer_data$found) {
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- scorer_data
        result$player_scored <- match_player_for_scorer(player_name, scorer_data$scorer)
      } else {
        result$error <- scorer_data$error
      }
      
    } else if (grepl("anytime.*td|anytime.*touchdown", market_lower)) {
      debug_cat("Market type: Anytime touchdown scorer\n")
      if (sport %in% c("nfl", "football")) {
        stats_data <- fetch_nfl_player_stats(player_name, result$season, game_id = game_id)
        if (!is.null(stats_data) && stats_data$found) {
          result$success <- TRUE
          result$resolved <- TRUE
          tds <- stats_data$stats$total_tds
          result$bet_won <- (tds > 0)
          result$actual_value <- tds
        } else {
          result$error <- stats_data$error %||% "Failed to fetch player stats"
        }
      }
      
    } else if (grepl("over|under", market_lower) && grepl("player", market_lower)) {
      debug_cat("Market type: Player prop over/under\n")
      if (sport %in% c("nfl", "football")) {
        stats_data <- fetch_nfl_player_stats(player_name, result$season, game_id = game_id)
      } else if (sport %in% c("nba", "basketball")) {
        stats_data <- fetch_nba_player_stats(player_name, result$season, game_date_obj, game_id)
      } else if (sport %in% c("mlb")) {
        stats_data <- fetch_mlb_player_stats(player_name, result$season, game_date_obj, game_id)
      } else {
        stats_data <- NULL
      }
      
      if (!is.null(stats_data) && stats_data$found) {
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- stats_data
        
        stat_type <- "unknown"
        if (grepl("passing.*yards", market_lower)) stat_type <- "passing_yards"
        else if (grepl("rushing.*yards", market_lower)) stat_type <- "rushing_yards"
        else if (grepl("receiving.*yards", market_lower)) stat_type <- "receiving_yards"
        else if (grepl("points", market_lower)) stat_type <- "points"
        else if (grepl("rebounds", market_lower)) stat_type <- "rebounds"
        else if (grepl("assists", market_lower)) stat_type <- "assists"
        else if (grepl("hits", market_lower)) stat_type <- "hits"
        else if (grepl("home.?runs|hr", market_lower)) stat_type <- "home_runs"
        
        actual_value <- stats_data$stats[[stat_type]]
        if (!is.na(actual_value)) {
          line_val_num <- as.numeric(line_value)
          if (grepl("over", market_lower)) result$bet_won <- actual_value > line_val_num
          else if (grepl("under", market_lower)) result$bet_won <- actual_value < line_val_num
          result$actual_value <- actual_value
          result$line_value <- line_val_num
        }
      } else {
        result$error <- stats_data$error %||% "Failed to fetch player stats"
      }
      
    } else if (grepl("moneyline|total|spread", market_lower)) {
      debug_cat("Market type: Game market\n")
      game_data <- fetch_game_result(game_id, sport, result$season)
      if (game_data$found) {
        result$success <- TRUE
        result$resolved <- TRUE
        result$data <- game_data
        
        if (grepl("moneyline", market_lower)) {
          bet_team <- if (grepl(teams$home, player_name, ignore.case=TRUE) || 
                          grepl(player_name, teams$home, ignore.case=TRUE)) "home" else "away"
          result$bet_won <- if (bet_team == "home") game_data$home_score > game_data$away_score
          else game_data$away_score > game_data$home_score
        } else if (grepl("total", market_lower)) {
          line_val_num <- as.numeric(line_value)
          if (grepl("over", market_lower)) result$bet_won <- game_data$total_points > line_val_num
          else if (grepl("under", market_lower)) result$bet_won <- game_data$total_points < line_val_num
          result$actual_value <- game_data$total_points
          result$line_value <- line_val_num
        }
      } else {
        result$error <- game_data$error
      }
      
    } else {
      error_msg <- "Unknown market type"
      result$error <- error_msg
      debug_cat(sprintf("ERROR: %s\n", error_msg))
    }
    
  }, error = function(e) {
    error_msg <- paste("Error in resolve_bet:", e$message)
    result$error <- error_msg
    debug_cat(sprintf("ERROR: %s\n", error_msg))
  })
  
  debug_cat("\n===============================================================================\n")
  debug_cat("RESOLUTION COMPLETE\n")
  debug_cat(sprintf("Success: %s\n", result$success))
  debug_cat(sprintf("Resolved: %s\n", result$resolved))
  debug_cat(sprintf("Final Season Used: %s\n", result$season))
  debug_cat(sprintf("Calculated Season: %s\n", result$calculated_season))
  if (result$fallback_season_used) {
    debug_cat(sprintf("Fallback Used: Yes (original calculated: %s)\n", result$original_calculated_season))
  }
  if (!is.null(result$error)) {
    debug_cat(sprintf("Error: %s\n", result$error))
  }
  if (!is.null(result$bet_won)) {
    debug_cat(sprintf("Bet won: %s\n", result$bet_won))
  }
  debug_cat("===============================================================================\n")
  
  return(result)
}

# ==============================================
# PERIOD/QUARTER/INNING DATA EXTRACTION FUNCTIONS
# ==============================================

extract_period_scores <- function(game_id, sport, period_type, period_num) {
  debug_cat(sprintf("  Extracting %s %d scores\n", period_type, period_num))
  
  tryCatch({
    if (sport %in% c("nba", "basketball")) {
      url <- paste0("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event=", game_id)
      response <- GET(url, timeout = 10)
      if (status_code(response) == 200) {
        data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
        if (!is.null(data$boxscore) && !is.null(data$boxscore$periodScores)) {
          period_scores <- data$boxscore$periodScores
          if (period_type == "quarter" && period_num <= nrow(period_scores)) {
            return(list(
              home_score = as.numeric(period_scores$homeScore[period_num] %||% 0),
              away_score = as.numeric(period_scores$awayScore[period_num] %||% 0),
              total = as.numeric(period_scores$homeScore[period_num] %||% 0) + 
                as.numeric(period_scores$awayScore[period_num] %||% 0)
            ))
          }
        }
      }
    } else if (sport %in% c("mlb", "baseball")) {
      url <- paste0("https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary?event=", game_id)
      response <- GET(url, timeout = 10)
      if (status_code(response) == 200) {
        data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
        if (!is.null(data$boxscore) && !is.null(data$boxscore$linescores)) {
          linescores <- data$boxscore$linescores
          if (is.data.frame(linescores) && period_num <= nrow(linescores)) {
            return(list(
              home_score = as.numeric(linescores$homeScore[period_num] %||% 0),
              away_score = as.numeric(linescores$awayScore[period_num] %||% 0),
              total = as.numeric(linescores$homeScore[period_num] %||% 0) + 
                as.numeric(linescores$awayScore[period_num] %||% 0)
            ))
          }
        }
      }
    }
    return(list(home_score = 0, away_score = 0, total = 0))
  }, error = function(e) {
    return(list(home_score = 0, away_score = 0, total = 0))
  })
}

extract_cumulative_scores <- function(game_id, sport, innings = NULL, quarters = NULL, periods = NULL) {
  result <- list(home_score = 0, away_score = 0, total = 0)
  
  if (!is.null(innings) && sport %in% c("mlb", "baseball")) {
    for (inning in 1:innings) {
      inning_scores <- extract_period_scores(game_id, sport, "inning", inning)
      result$home_score <- result$home_score + inning_scores$home_score
      result$away_score <- result$away_score + inning_scores$away_score
    }
  }
  
  if (!is.null(quarters) && sport %in% c("nba", "basketball")) {
    for (quarter in 1:quarters) {
      quarter_scores <- extract_period_scores(game_id, sport, "quarter", quarter)
      result$home_score <- result$home_score + quarter_scores$home_score
      result$away_score <- result$away_score + quarter_scores$away_score
    }
  }
  
  if (!is.null(periods) && sport %in% c("nhl", "hockey")) {
    period_data <- get_espn_nhl_period_data(game_id)
    if (period_data$success) {
      for (p in 1:min(periods, length(period_data$periods))) {
        result$home_score <- result$home_score + (period_data$periods[[p]]$home_score %||% 0)
        result$away_score <- result$away_score + (period_data$periods[[p]]$away_score %||% 0)
      }
    }
  }
  
  result$total <- result$home_score + result$away_score
  return(result)
}

# ==============================================
# PERIOD/INNING MARKET RESOLVERS
# ==============================================
resolve_period_moneyline <- function(game_id, sport, period, bet_team = NULL, is_3way = FALSE) {
  debug_cat(sprintf("  Resolving %s moneyline (3-way: %s)\n", period, is_3way))
  
  if (grepl("half", period)) {
    half_num <- ifelse(grepl("1st", period), 1, 2)
    scores <- extract_cumulative_scores(game_id, sport,
                                        quarters = if(sport %in% c("nba", "basketball")) half_num * 2 else NULL,
                                        periods = if(sport %in% c("nhl", "hockey")) half_num * 2 else NULL,
                                        innings = if(sport %in% c("mlb", "baseball")) half_num * 4.5 else NULL)
  } else if (grepl("quarter", period)) {
    quarter_num <- as.numeric(gsub("quarter_", "", period))
    scores <- extract_period_scores(game_id, sport, "quarter", quarter_num)
  } else if (grepl("inning", period)) {
    inning_num <- as.numeric(gsub("inning_", "", period))
    scores <- extract_period_scores(game_id, sport, "inning", inning_num)
  } else {
    scores <- list(home_score = 0, away_score = 0)
  }
  
  result <- list(home_score = scores$home_score, away_score = scores$away_score, 
                 total = scores$home_score + scores$away_score)
  
  if (scores$home_score > scores$away_score) {
    result$winner <- "home"
  } else if (scores$away_score > scores$home_score) {
    result$winner <- "away"
  } else {
    result$winner <- "tie"
  }
  
  if (!is.null(bet_team)) {
    result$bet_won <- (result$winner == bet_team)
  } else if (is_3way && result$winner == "tie") {
    result$bet_won <- TRUE
  }
  
  return(result)
}

resolve_period_total <- function(game_id, sport, period, line_value, direction) {
  debug_cat(sprintf("  Resolving %s total: %s %s\n", period, direction, line_value))
  
  period_result <- resolve_period_moneyline(game_id, sport, period)
  total <- period_result$total
  line_num <- as.numeric(line_value)
  
  result <- list(actual_value = total, line_value = line_num)
  
  if (direction == "over") {
    result$bet_won <- total > line_num
  } else if (direction == "under") {
    result$bet_won <- total < line_num
  } else if (direction == "odd") {
    result$bet_won <- (total %% 2) == 1
  } else if (direction == "even") {
    result$bet_won <- (total %% 2) == 0
  }
  
  return(result)
}

# ========== ADD THE TWO FUNCTIONS HERE ==========

resolve_period_spread <- function(game_id, sport, period, line_value, bet_team) {
  debug_cat(sprintf("  Resolving %s spread: %s @ %s\n", period, bet_team, line_value))
  
  period_result <- resolve_period_moneyline(game_id, sport, period)
  point_diff <- period_result$home_score - period_result$away_score
  line_num <- as.numeric(line_value)
  
  result <- list(
    point_diff = point_diff, 
    line_value = line_num,
    home_score = period_result$home_score, 
    away_score = period_result$away_score
  )
  
  if (bet_team == "home") {
    result$bet_won <- (point_diff + line_num) > 0
  } else if (bet_team == "away") {
    result$bet_won <- (-point_diff + line_num) > 0
  } else {
    result$bet_won <- FALSE
  }
  
  return(result)
}

resolve_period_both_teams_score <- function(game_id, sport, period) {
  debug_cat(sprintf("  Resolving %s both teams to score\n", period))
  
  period_result <- resolve_period_moneyline(game_id, sport, period)
  
  result <- list(
    both_scored = (period_result$home_score > 0 && period_result$away_score > 0),
    home_score = period_result$home_score, 
    away_score = period_result$away_score
  )
  
  result$bet_won <- result$both_scored
  
  return(result)
}

# ========== CONTINUE WITH INNING FUNCTIONS ==========

resolve_inning_moneyline <- function(game_id, inning, bet_team = NULL, is_3way = FALSE) {
  return(resolve_period_moneyline(game_id, "mlb", paste0("inning_", inning), bet_team, is_3way))
}

resolve_inning_total_runs <- function(game_id, inning, line_value, direction) {
  return(resolve_period_total(game_id, "mlb", paste0("inning_", inning), line_value, direction))
}

resolve_multi_inning_market <- function(game_id, num_innings, market_type, line_value = NULL, direction = NULL, bet_team = NULL) {
  debug_cat(sprintf("  Resolving first %d innings %s\n", num_innings, market_type))
  
  cumulative <- extract_cumulative_scores(game_id, "mlb", innings = num_innings)
  
  result <- list(home_score = cumulative$home_score, away_score = cumulative$away_score,
                 total = cumulative$total, innings = num_innings)
  
  if (market_type == "moneyline") {
    if (cumulative$home_score > cumulative$away_score) {
      result$winner <- "home"
    } else if (cumulative$away_score > cumulative$home_score) {
      result$winner <- "away"
    } else {
      result$winner <- "tie"
    }
    if (!is.null(bet_team)) result$bet_won <- (result$winner == bet_team)
  } else if (market_type == "total_runs") {
    line_num <- as.numeric(line_value)
    result$bet_won <- if (direction == "over") cumulative$total > line_num else cumulative$total < line_num
    result$actual_value <- cumulative$total
    result$line_value <- line_num
  } else if (market_type == "both_teams_score") {
    result$both_scored <- (cumulative$home_score > 0 && cumulative$away_score > 0)
    result$bet_won <- result$both_scored
  }
  
  return(result)
}

# ==============================================
# SPECIAL GAME RESOLVERS
# ==============================================

resolve_game_overtime <- function(game_id, sport) {
  debug_cat("  Resolving will there be overtime\n")
  
  if (sport %in% c("nba", "basketball")) {
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event=", game_id)
    response <- GET(url, timeout = 10)
    if (status_code(response) == 200) {
      data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
      if (!is.null(data$boxscore) && !is.null(data$boxscore$periodScores)) {
        num_periods <- nrow(data$boxscore$periodScores)
        return(list(bet_won = num_periods > 4, periods = num_periods))
      }
    }
  } else if (sport %in% c("nhl", "hockey")) {
    period_data <- get_espn_nhl_period_data(game_id)
    if (period_data$success) {
      return(list(bet_won = period_data$game_data$went_to_ot %||% FALSE))
    }
  }
  return(list(bet_won = FALSE, error = "Could not determine overtime status"))
}

resolve_game_extra_innings <- function(game_id) {
  debug_cat("  Resolving will there be extra innings\n")
  
  url <- paste0("https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary?event=", game_id)
  response <- GET(url, timeout = 10)
  
  if (status_code(response) == 200) {
    data <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    if (!is.null(data$boxscore) && !is.null(data$boxscore$periodScores)) {
      num_innings <- nrow(data$boxscore$periodScores)
      return(list(bet_won = num_innings > 9, innings = num_innings))
    }
  }
  return(list(bet_won = FALSE, error = "Could not determine extra innings status"))
}

# ==============================================
# MAIN EXECUTION
# ==============================================

main <- function() {
  debug_cat("\n=== MAIN EXECUTION START ===\n")
  
  tryCatch({
    # Get command line arguments
    args <- commandArgs(trailingOnly = TRUE)
    
    if (length(args) >= 4) {
      player_name <- args[1]
      sport <- tolower(args[2])
      season <- as.numeric(args[3])
      market_type <- args[4]
      event_string <- if (length(args) >= 5) args[5] else ""
      
      sport <- normalize_sport_name(sport)
      
      # Handle line_value (arg 6)
      line_value <- if (length(args) >= 6) {
        if (args[6] == "NULL" || args[6] == "null" || nchar(args[6]) == 0) {
          NULL
        } else {
          args[6]
        }
      } else NULL
      
      # Handle game_date (arg 7) from Python
      game_date_arg <- if (length(args) >= 7) {
        if (args[7] == "NULL" || args[7] == "null" || nchar(args[7]) == 0) {
          NULL
        } else {
          args[7]
        }
      } else NULL
      
      debug_cat(sprintf("DEBUG CLI: Received %d arguments\n", length(args)))
      debug_cat(sprintf("  Player: %s\n", player_name))
      debug_cat(sprintf("  Sport: %s\n", sport))
      debug_cat(sprintf("  Season: %s\n", season))
      debug_cat(sprintf("  Market: %s\n", market_type))
      debug_cat(sprintf("  Event: %s\n", event_string))
      debug_cat(sprintf("  Line: %s\n", line_value))
      debug_cat(sprintf("  Game Date: %s\n", game_date_arg))
      debug_cat(sprintf("  System Date: %s\n", Sys.Date()))
      
      # Resolve the bet
      resolution <- resolve_bet(player_name, sport, season, market_type, event_string, line_value, game_date_arg)
      
      # ========== CRITICAL: RESTORE STDOUT FOR JSON OUTPUT ==========
      # Close stderr sinks
      while (sink.number() > 0) sink()
      
      # Output JSON to stdout (ONLY THIS GOES TO STDOUT)
      cat("\n")  # Ensure JSON starts on new line
      cat(toJSON(resolution, auto_unbox = TRUE, null = "null", na = "null"))
      cat("\n")  # Ensure JSON ends with new line
      
    } else {
      # Show usage information
      debug_cat("Usage: Rscript bet_resolver.R \"player_name\" \"sport\" \"season\" \"market_type\" \"event_string\" [line_value] [game_date]\n")
      debug_cat("Arguments (7 total, last 3 optional):\n")
      debug_cat("  1. player_name  : Player name (e.g., \"Patrick Mahomes\")\n")
      debug_cat("  2. sport        : Sport (\"nfl\", \"nba\", or \"nhl\")\n")
      debug_cat("  3. season       : Season year (e.g., 2024)\n")
      debug_cat("  4. market_type  : Market type (e.g., \"player passing yards over\")\n")
      debug_cat("  5. event_string : Event description (e.g., \"Chiefs @ Dolphins\")\n")
      debug_cat("  6. line_value   : Line value (e.g., 275.5) or NULL\n")
      debug_cat("  7. game_date    : Game date in YYYY-MM-DD format (optional, overrides extraction from event)\n")
    }
    
    debug_cat("\n=== MAIN EXECUTION COMPLETE ===\n")
    
  }, error = function(e) {
    # Close all sinks on error
    while (sink.number() > 0) sink()
    
    error_result <- list(
      success = FALSE,
      error = paste("Fatal error:", e$message),
      stack_trace = paste(capture.output(traceback()), collapse = "\n")
    )
    
    cat(toJSON(error_result, auto_unbox = TRUE, null = "null", na = "null"))
  })
}

# ==============================================
# TEST FUNCTION
# ==============================================

run_all_tests <- function() {
  cat("==================================================\n")
  cat("COMPREHENSIVE TEST SUITE FOR bet_resolver.R\n")
  cat("Date: ", Sys.Date(), "\n")
  cat("==================================================\n\n")
  
  # Test 1: Normalization
  cat("1. Testing normalize_sport_name()...\n")
  tests <- list(
    c("nfl", "nfl"),
    c("NFL", "nfl"),
    c("football", "nfl"),
    c("nba", "nba"),
    c("NBA", "nba"),
    c("basketball", "nba"),
    c("NFL Football", "nfl"),
    c("American Football", "nfl"),
    c("nhl", "nhl"),
    c("NHL", "nhl"),
    c("hockey", "nhl"),
    c("ice hockey", "nhl")
  )
  
  all_pass <- TRUE
  for (test in tests) {
    result <- normalize_sport_name(test[1])
    status <- ifelse(result == test[2], "✓ PASS", "✗ FAIL")
    if (result != test[2]) all_pass <- FALSE
    cat(sprintf("  %-20s -> %-10s: %s\n", test[1], result, status))
  }
  cat(sprintf("  Result: %s\n\n", ifelse(all_pass, "All passed", "Some failed")))
  
  # Test 2: Find game ID
  cat("2. Testing find_game_id()...\n")
  cat("   Testing Pacers @ Celtics on Dec 14, 2025...\n")
  
  game_id <- find_game_id(
    home_team = "Boston Celtics",
    away_team = "Indiana Pacers",
    game_date = "2025-12-14",
    sport = "nba"
  )
  
  if (!is.na(game_id)) {
    cat(sprintf("  ✓ Found game ID: %s\n", game_id))
    cat("  Verifying game exists...\n")
    
    game_status <- check_nba_game_status(game_id)
    if (game_status$exists) {
      cat(sprintf("  ✓ Game verified: %s @ %s\n",
                  game_status$away_team, game_status$home_team))
      cat(sprintf("    Status: %s (completed: %s)\n",
                  game_status$status, game_status$completed))
    } else {
      cat(sprintf("  ✗ Game not found via verification: %s\n", game_status$error))
    }
  } else {
    cat("  ✗ Could not find game ID\n")
  }
  
  cat("\n")
  
  # Test 3: Resolve bet
  cat("3. Testing resolve_bet()...\n")
  result <- resolve_bet(
    player_name = "Jayson Tatum",
    sport = "nba",
    season = 2025,
    market_type = "player points over",
    event_string = "Indiana Pacers @ Boston Celtics",
    line_value = "27.5",
    game_date = "2025-12-14"
  )
  
  if (result$success) {
    cat("✓ Bet resolved successfully!\n")
    cat(sprintf("  Game ID: %s\n", result$game_id))
    cat(sprintf("  Season used: %s\n", result$season))
    if (!is.null(result$bet_won)) {
      cat(sprintf("  Bet won: %s\n", result$bet_won))
      cat(sprintf("  Actual points: %s\n", result$actual_value))
      cat(sprintf("  Line: %s\n", result$line_value))
    }
  } else {
    cat(sprintf("✗ Failed: %s\n", result$error))
  }
  
  cat("\n==================================================\n")
  cat("TEST SUITE COMPLETE\n")
  cat("==================================================\n")
}

# ==============================================
# NHL TEST FUNCTION
# ==============================================

test_nhl_markets <- function() {
  cat("\n==================================================\n")
  cat("NHL MARKET TEST SUITE\n")
  cat("Date: ", as.character(Sys.Date()), "\n")
  cat("==================================================\n\n")
  
  # Test NHL market classification
  cat("1. Testing NHL market classification...\n")
  nhl_test_cases <- list(
    list(market = "Player Goals", expected = "player_stat"),
    list(market = "Player Assists", expected = "player_stat"),
    list(market = "Player Points", expected = "player_stat"),
    list(market = "Player Shots On Goal", expected = "player_stat"),
    list(market = "Player Saves", expected = "player_stat"),
    list(market = "Player 1st Period Goals", expected = "player_period_stat"),
    list(market = "1st Period Total Goals", expected = "period_total_goals"),
    list(market = "2nd Period Total Goals", expected = "period_total_goals"),
    list(market = "3rd Period Moneyline", expected = "period_moneyline"),
    list(market = "Moneyline", expected = "game_moneyline"),
    list(market = "Puck Line", expected = "game_puckline"),
    list(market = "Total Goals", expected = "game_total_goals"),
    list(market = "Both Teams To Score", expected = "game_both_teams_score"),
    list(market = "Regulation Total Goals", expected = "regulation_total_goals"),
    list(market = "Player First Goal", expected = "first_scorer")
  )
  
  all_pass <- TRUE
  for (test in nhl_test_cases) {
    result <- classify_nhl_market(tolower(test$market))
    status <- ifelse(result$type == test$expected, "✓ PASS", "✗ FAIL")
    if (result$type != test$expected) all_pass <- FALSE
    cat(sprintf("  %-30s -> %-25s %s\n", test$market, result$type, status))
  }
  cat(sprintf("  Result: %s\n\n", ifelse(all_pass, "All passed", "Some failed")))
  
  cat("==================================================\n")
  cat("NHL TEST SUITE COMPLETE\n")
  cat("==================================================\n")
}


# Run main function
if (!interactive()) {
  main()
}