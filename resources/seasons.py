from flask_restful import Resource

import sqlite3 as sq
import pandas as pd
import numpy as np
import utils

class AllSeasons(Resource):
    def get(self):
        con = sq.connect("stats.db")
        cur = con.cursor()

        cur.execute(f"""
        SELECT * from champions
         """)
        
        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        res = [dict(zip(keys, row)) for row in rows]
        res = [year for year in res if year['year'] not in ['08-09', '09-10', '10-11']]
        for season in res:
            season['year'] = f"<a href = '../seasons/{season['year']}'>{season['year']}</a>"
            
        return pd.DataFrame(res).pivot(columns='tournament', values='school', index='year').reset_index().to_dict('records')
    
class SeasonList(Resource):
    def get(self):
        con = sq.connect("stats.db")
        cur = con.cursor()

        cur.execute(f"""
        SELECT distinct sets.year from sets
         """)
        res = cur.fetchall()

        paths = [
            {
                'slug': player[0]
            }
            for player in res
        ]

        return paths


class Season(Resource):
    def get(self, season):
        con = sq.connect("stats.db")
        cur = con.cursor()

        cur.execute(f"""
        SELECT 
sets.year as Season, tournament as Tournament, champions.tournament_id, \"set\", site, team, players
from champions
LEFT JOIN tournaments on champions.tournament_id = tournaments.tournament_id
LEFT JOIN sets on tournaments.set_id = sets.set_id
LEFT JOIN sites on tournaments.site_id = sites.site_id
LEFT JOIN (SELECT player_games.tournament_id, team_id, 
           group_concat(distinct ' ' || coalesce(fname|| ' ' || lname, player_games.player)) as players
            from player_games
            LEFT JOIN schools on player_games.school_id = schools.school_id
            LEFT JOIN players on player_games.player_id = players.player_id
            LEFT JOIN people on players.person_id = people.person_id
            LEFT JOIN tournaments on player_games.tournament_id = tournaments.tournament_id
            LEFT JOIN sets on tournaments.set_id = sets.set_id
            WHERE year = '{season}'
            GROUP BY 1, 2) player_games
           on champions.tournament_id = player_games.tournament_id
           and champions.team_id = player_games.team_id
where champions.year = '{season}'
         """)
        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        champs_res = [dict(zip(keys, row)) for row in rows]
        for tournament in champs_res:
            tournament['Tournament'] = f"<a href = '../tournaments/{str(tournament['tournament_id']).replace('.0', '')}'>{tournament['Tournament']}</a>"
            tournament['team'] = f"<a href = '../tournaments/{str(tournament['tournament_id']).replace('.0', '')}#{utils.slug(tournament['team'])}'>{tournament['team']}</a>"

        cur.execute(f"""SELECT tournaments.*, results.Champion FROM (SELECT 
                    date as Date,
                    sets.year, \"set\" as \"Set\", site as Site,
                    \"set\" || ' at ' || site as Tournament, tournaments.tournament_id,
                    count(distinct team_games.team_id) as Teams,
                    count(distinct team_games.school_id) as Schools
                    from team_games
                    left join teams on team_games.team_id = teams.team_id
                    left join tournaments on team_games.tournament_id = tournaments.tournament_id
                    LEFT JOIN sets on tournaments.set_id = sets.set_id
                    LEFT JOIN sites on tournaments.site_id = sites.site_id
                    WHERE sets.year = '{season}'
                    GROUP BY 1, 2, 3, 4, 5, 6
                    ORDER BY Date) tournaments
                    LEFT JOIN
                    (
                        SELECT 
                        date as Date,
                        \"set\" || ' at ' || site as Tournament,
                        teams.team as Champion
                        from team_games
                        left join teams on team_games.team_id = teams.team_id
                        left join tournaments on team_games.tournament_id = tournaments.tournament_id
                        LEFT JOIN sets on tournaments.set_id = sets.set_id
                        LEFT JOIN sites on tournaments.site_id = sites.site_id
                        LEFT JOIN tournament_results on team_games.tournament_id = tournament_results.tournament_id
                        and team_games.team_id = tournament_results.team_id
                        WHERE sets.year = '{season}'
                        and rank = 1
                        GROUP BY 1, 2
                        ORDER BY Date desc
                    ) results
                    on tournaments.Tournament = results.Tournament
 """)
        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        tournament_res = [dict(zip(keys, row)) for row in rows]
        for tournament in tournament_res:
            tournament['Tournament'] = f"<a href = '../tournaments/{str(tournament['tournament_id']).replace('.0', '')}'>{tournament['Tournament']}</a>"

        cur.execute(f"""SELECT 
sets.\"set\" as \"Set\", set_slug, difficulty, 
                    count(distinct team_games.site_id) as Sites,
                    count(distinct team_games.team_id) as Teams,
                    count(distinct team_games.school_id) as Schools
from team_games
 LEFT JOIN sets on team_games.set_id = sets.set_id
 LEFT JOIN sites on team_games.site_id = sites.site_id
left join editors on sets.set_id = editors.set_id
 LEFT JOIN schools on team_games.school_id = schools.school_id
 LEFT JOIN teams on team_games.team_id = teams.team_id
 WHERE sets.year = '{season}'
 GROUP BY 1,2,3
 order by difficulty, Teams
 """)
        diff_level = {
            'easy': 1,
            'medium': 2,
            'regionals': 3,
            'nationals': 4
        }
            
        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        sets_res = [dict(zip(keys, row)) for row in rows]
        sets_res = pd.DataFrame(sets_res)
        sets_res['difficulty_lev'] = [diff_level[diff] for diff in sets_res['difficulty']]
        sets_res = pd.DataFrame(sets_res).sort_values(['difficulty_lev', 'Set']).to_dict('records')
        print(sets_res)
        for set in sets_res:
            set['Set'] = f"<a href = '../sets/{set['set_slug']}'>{set['Set']}</a>"
            if set['difficulty'] == 'easy':
                set['difficulty'] = "<div class = 'diffdots easy-diff'>&#x25CF;</div>"
            elif set['difficulty'] == 'medium':
                set['difficulty'] = "<div class = 'diffdots medium-diff'>&#x25CF;&#x25CF;</div>"
            elif set['difficulty'] == 'regionals':
                set['difficulty'] = "<div class = 'diffdots regionals-diff'>&#x25CF;&#x25CF;&#x25CF;</div>"
            else:
                set['difficulty'] = "<div class = 'diffdots nationals-diff'>&#x25CF;&#x25CF;&#x25CF;&#x25CF;</div>"

        return {'Champions': champs_res,
                'Tournaments': tournament_res,
                'Sets': sets_res}
