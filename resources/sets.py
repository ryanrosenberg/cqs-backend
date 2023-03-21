from flask_restful import Resource

import sqlite3 as sq
import pandas as pd
import numpy as np
import utils

class AllEditors(Resource):
    def get(self):
        con = sq.connect("../stats.db")
        cur = con.cursor()

        def read_df(table_name):
            cur.execute(f"SELECT * from {table_name}")
            rows = cur.fetchall()
            keys = [k[0] for k in cur.description]
            res = [dict(zip(keys, row)) for row in rows]

            return pd.DataFrame(res)

        editors = pd.read_csv('https://github.com/ryanrosenberg/cqs-backend/blob/master/all-editors.csv').replace({np.nan:None}).to_dict(orient='records')

        return editors

class SetList(Resource):
    def get(self):
        con = sq.connect("stats.db")
        cur = con.cursor()

        cur.execute(f"""
        SELECT sets.set_slug from sets """)
        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        res = [dict(zip(keys, row)) for row in rows]

        return res

class Set(Resource):
    def get(self, set_slug):
        con = sq.connect("stats.db")
        cur = con.cursor()

        cur.execute(f"""
        SELECT 
sets.year as Year,
sets.\"set\" as \"Set\", difficulty, category, set_name, headitor
from sets
left join editors on sets.set_id = editors.set_id
 WHERE sets.set_slug = '{set_slug}'
 """)
        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        summary_res = [dict(zip(keys, row)) for row in rows]

        cur.execute(f"""
        SELECT 
sets.year as Year,
sets.\"set\" as \"Set\", difficulty, set_name, headitor,
category, subcategory as Subcategory, editor as Editor, slug
from sets
LEFT JOIN editors on sets.set_id = editors.set_id
LEFT JOIN people on editors.person_id = people.person_id
 WHERE sets.set_slug = '{set_slug}'
 and category != 'Head'
 """)
        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        editor_res = [dict(zip(keys, row)) for row in rows]
        for editor in editor_res:
            if editor['slug']:
                editor['Editor'] = f"<a href = '../players/{editor['slug']}'>{editor['Editor']}</a>"
        
        if len(editor_res) > 0:
            editor_res = pd.DataFrame(editor_res).groupby(
                ['category', 'Subcategory']
            )[['Editor']].agg(
                lambda x: ', '.join(x)
            ).reset_index().to_dict('records')

        cur.execute(f"""SELECT 
sets.year as Year, tournaments.date as Date, team_games.tournament_id,
\"set\" as \"Set\", site as Site, count(distinct team_id) as Teams,
count(distinct(team_games.school_id)) as Schools,
round(sum(powers)/sum(ifnull(tuh, 20)), 3) as \"15%\",
printf("%.3f",(sum(ifnull(powers,0))+sum(tens))/sum(ifnull(tuh, 20)/2)) as \"Conv%\",
printf("%.2f",sum(bonus_pts)/sum(bonuses_heard)) as PPB
from team_games
LEFT JOIN tournaments on team_games.tournament_id = tournaments.tournament_id
 LEFT JOIN sets on tournaments.set_id = sets.set_id
 LEFT JOIN sites on tournaments.site_id = sites.site_id
 WHERE sets.set_slug = '{set_slug}'
 GROUP BY 1,2,3,4
 """)
        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        tournament_res = [dict(zip(keys, row)) for row in rows]
        for en in tournament_res:
                en['Site'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Site']}</a>"

        cur.execute(f"""SELECT 
sets.year as Year,
ifnull(team, school_name) as Team, team_games.tournament_id, schools.slug as school_slug,
\"set\" as \"Set\", site as Site, 
count(game_id) as GP,
 sum(case result when 1 then 1 else 0 end) || '-' ||
 sum(case result when 0 then 1 else 0 end) as \"W-L\",
 sum(ifnull(tuh, 20)) as TUH,
 sum(powers) as \"15\", sum(tens) as \"10\", sum(negs) as \"-5\",
 printf("%.1f", sum(powers)/count(result)) as \"15/G\",
 printf("%.1f", sum(tens)/count(result)) as \"10/G\",
 printf("%.1f", sum(negs)/count(result)) as \"-5/G\",
 printf("%.3f",(sum(ifnull(powers, 0)) + sum(tens))/sum(ifnull(tuh, 20)), 3) as \"TU%\",
 printf("%.2f", avg(total_pts)) as PPG, 
 printf("%.2f", sum(bonus_pts)/sum(bonuses_heard)) as PPB,
 printf("%.2f",a_value) as \"A-Value\" 
from team_games
 LEFT JOIN sets on team_games.set_id = sets.set_id
 LEFT JOIN sites on team_games.site_id = sites.site_id
 LEFT JOIN schools on team_games.school_id = schools.school_id
 LEFT JOIN teams on team_games.team_id = teams.team_id
 LEFT JOIN tournament_results on team_games.tournament_id = tournament_results.tournament_id
           and team_games.team_id = tournament_results.team_id
 WHERE sets.set_slug = '{set_slug}'
 GROUP BY 1,2,3,4,5
 """)
        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        teams_res = [dict(zip(keys, row)) for row in rows]
        for team in teams_res:
            team['Site'] = f"<a href = '../tournaments/{str(team['tournament_id']).replace('.0', '')}'>{team['Site']}</a>"
            team['Team'] = f"<a href = '../tournaments/{str(team['tournament_id']).replace('.0', '')}#{utils.slug(team['Team'])}'>{team['Team']}</a>"

        cur.execute(f"""
           SELECT
           sets.year as Year, \"set\" as \"Set\", site as Site, player_games.tournament_id,
           coalesce(fname|| ' ' || lname, player_games.player) as Player,
           team as Team, slug,
           count(tens) as GP,
           sum(ifnull(tuh, 20)) as TUH,
           sum(powers) as \"15\", 
           sum(tens) as \"10\", 
           sum(negs) as \"-5\",
           printf("%.1f", sum(powers)/count(tens)) as \"15/G\",
           printf("%.1f", sum(tens)/count(tens)) as \"10/G\",
           printf("%.1f", sum(negs)/count(tens)) as \"-5/G\",
           printf("%.2f", sum(powers)/sum(negs)) as \"P/N\",
           printf("%.2f", (sum(ifnull(powers, 0)) + sum(tens))/sum(negs)) as \"G/N\",
           printf("%.2f", avg(pts)) as PPG from
           player_games
           LEFT JOIN teams on player_games.team_id = teams.team_id
           LEFT JOIN tournaments on player_games.tournament_id = tournaments.tournament_id
           LEFT JOIN sets on player_games.set_id = sets.set_id
           LEFT JOIN sites on player_games.site_id = sites.site_id
           LEFT JOIN players on player_games.player_id = players.player_id
           LEFT JOIN people on players.person_id = people.person_id
 WHERE sets.set_slug = '{set_slug}'
 GROUP BY 1,2,3,4,5, 6
 ORDER BY Player
 """)
        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        players_res = [dict(zip(keys, row)) for row in rows]
        for player in players_res:
            if player['slug']:
                player['Player'] = f"<a href = '../players/{player['slug']}'>{player['Player']}</a>"
            player['Team'] = f"<a href = '../tournaments/{str(player['tournament_id']).replace('.0', '')}#{utils.slug(player['Team'])}'>{player['Team']}</a>"


        return({
            'Summary': summary_res,
            'Editors': editor_res,
            'Tournaments': tournament_res,
            'Teams': teams_res,
            'Players': players_res
        })
