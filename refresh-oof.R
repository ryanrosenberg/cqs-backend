
library(tidyverse)
library(DBI)
library(googlesheets4)
library(rvest)

con <- dbConnect(RSQLite::SQLite(), "Documents/qb/cqs-backend/stats.db")
dates <- read_sheet('https://docs.google.com/spreadsheets/d/1fkrZUmp8AMtAocJwXOU2p3IXXWXASUs087OJubaH6C8/edit#gid=762186562',
                    sheet = 'tournament_urls')

champions <- read_sheet('https://docs.google.com/spreadsheets/d/1fkrZUmp8AMtAocJwXOU2p3IXXWXASUs087OJubaH6C8/edit#gid=199300754',
                        sheet = 'champions')

og_tupptuh <- tbl(con, "team_games") %>%
  distinct(tournament_id, team_id, opponent_id) %>%
  left_join(tbl(con, "team_games"), by = c("tournament_id", 'team_id')) %>%
  filter(opponent_id.x != opponent_id.y) %>%
  group_by(tournament_id, team_id, opponent_id.x) %>%
  summarize(tupptuh = sum(total_pts)/sum(coalesce(tuh, 20))) %>%
  distinct() %>%
  rename(opponent_id = team_id, team_id = opponent_id.x) %>%
  collect()

a_value_stats <- tbl(con, "team_games") %>%
  group_by(tournament_id, team_id) %>%
  summarize(tupptuh = (15*sum(coalesce(powers, 0)) + 10*sum(tens) - 5*sum(negs))/sum(coalesce(tuh, 20)),
            bhptuh = sum(bonuses_heard)/sum(coalesce(tuh, 20)),
            PPB = sum(bonus_pts)/sum(bonuses_heard)) %>%
  collect() %>%
  left_join(dbReadTable(con, "team_games") %>%
              tibble() %>%
              left_join(og_tupptuh) %>%
              filter(!is.na(tupptuh)) %>%
              group_by(set_id) %>%
              mutate(tourn_pptuh = sum(total_pts)/sum(coalesce(tuh, 20))) %>%
              group_by(tournament_id, team_id) %>%
              summarize(sos = mean(tupptuh)/mean(tourn_pptuh))) %>%
  mutate(a_value = 20*(tupptuh*sos+bhptuh*sos*PPB)) %>%
  ungroup()

dates %>%
  filter(!is.na(url),
         !type %in% c('hsqb-ng', 'neg5') | is.na(type),
         standings != F | is.na(standings)) %>%
  mutate(standings_url = ifelse(is.na(standings_url),
                                str_replace(url, '_games.html$', '_standings.html'),
                                standings_url),
         standings_url = str_remove(standings_url, '/games/$')) %>%
  distinct(tournament_id, standings_url) %>%
  filter(!glue::glue('standings/{tournament_id}.html') %in% Sys.glob('standings/*')) %>%
  arrange(tournament_id) %>%
  pwalk(function(standings_url, tournament_id){
    print(tournament_id)
    xml2::write_html(read_html(standings_url), glue::glue('standings/{tournament_id}.html'))
  })

get_order_of_finish <- function(url, tournament_id){
  print(url)
  full_tbl <- url %>%
    read_html() %>%
    html_table() %>%
    discard(~dim(.)[1]==0) %>%
    discard(~dim(.)[2] <= 3) %>%
    discard(~dim(.)[2] > 25) %>%
    discard(~.[[1,2]]=="Individuals") %>%
    imap(~mutate(.x, bracket = .y)) %>%
    reduce(rbind)
  
  full_tbl <- full_tbl %>%
    select(2, bracket)
  
  full_tbl %>%
    set_names(c("Team", "bracket")) %>%
    filter(Team != "Team") %>%
    mutate(tournament_id = tournament_id,
           rank = row_number(),
           .before = 1)
}

oofs <- dates %>%
  filter(!is.na(url),
         !type %in% c('hsqb-ng', 'neg5') | is.na(type),
         standings != F | is.na(standings)) %>%
  mutate(local_html = glue::glue('standings/{tournament_id}.html')) %>%
  select(tournament_id, url = local_html)

safe_oof <- safely(get_order_of_finish)

try <- oofs %>%
  split(.$tournament_id) %>%
  map(pmap, safe_oof)

hsqb_oofs <- try %>%
  map_df(~.[[1]]$result)

results <- hsqb_oofs %>%
  rename(raw_team = Team) %>%
  left_join(team_lookup) %>%
  mutate(team_id = case_when(is.na(team_id) &
                               str_detect(raw_team, "WUSTL") ~ '913',
                             is.na(team_id) &
                               str_detect(raw_team, "Pikachu") ~ '912',
                             is.na(team_id) &
                               str_detect(raw_team, "Pavlov") ~ '911',
                             T ~ team_id)) %>%
  left_join(a_value_stats) %>%
  select(tournament_id, team_id, tupptuh:a_value, rank, bracket) %>%
  group_by(tournament_id) %>%
  mutate(num_teams = n()) %>%
  ungroup()

dbReadTable(con, "tournament_results") %>% 
  select(tournament_id, team_id, rank, bracket) %>% 
  mutate(team_id = ifelse(team_id == '798-1', '890-1', team_id)) %>% 
  as_tibble() %>% 
  full_join(a_value_stats) %>% 
  left_join(dbReadTable(con, "team_games") %>%
              as_tibble() %>% 
              group_by(tournament_id, team_id) %>% 
              summarize(G = n(),
                        W = sum(result == 1),
                        L = sum(result == 0))) %>% 
  dbWriteTable(con, "tournament_results", ., overwrite = T)

champions %>%
  dbWriteTable(con, "champions", ., overwrite = T)

dbReadTable(con, "team_games") %>%
  as_tibble() %>%
  distinct(tournament_id, game_id, round, game_num) %>%
  dbWriteTable(con, "games", ., overwrite = T)

dbReadTable(con, "games") %>%
  as_tibble()
