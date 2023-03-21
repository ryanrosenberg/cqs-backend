from flask_restful import Resource
import sqlite3 as sq
import pandas as pd
import numpy as np
import utils


class CircuitList(Resource):
    def get(self):
        con = sq.connect("stats.db")
        cur = con.cursor()
        cur.execute(
            """SELECT distinct circuit from schools 
            where circuit is not null
            and circuit <> 'National'
            and circuit <> 'Asia'""")
        res = cur.fetchall()
        paths = [
            {
                'slug': utils.slug(circuit[0]),
                'name': circuit[0]
            }
            for circuit in res
        ]

        return paths


class Circuit(Resource):
    def get(self, circuit_slug):
        print(circuit_slug)
        con = sq.connect("stats.db")
        cur = con.cursor()
        cur.execute(f"""
        SELECT 
school_name as School, slug as school_slug, schools.lat, schools.lon, schools.school_id,
min(strftime('%Y', tournaments.date)) || '-' || max(strftime('%Y', tournaments.date)) as Yrs,
ifnull(cast(max(strftime('%Y', tournaments.date)) as int), 2000) as last_active,
count(distinct tournaments.tournament_id) as Ts,
count(game_id) as GP,
 sum(case result when 1 then 1 else 0 end) || '-' ||
 sum(case result when 0 then 1 else 0 end) as \"W-L\",
 printf("%.3f", avg(result)) as \"Win%\",
 sum(ifnull(tuh, 20)) as TUH,
 sum(powers) as \"15\", sum(tens) as \"10\", sum(negs) as \"-5\",
 printf("%.2f", sum(powers)/count(result)) as \"15/G\",
 printf("%.2f", sum(tens)/count(result)) as \"10/G\",
 printf("%.2f", sum(negs)/count(result)) as \"-5/G\",
 printf("%.3f", (sum(ifnull(powers, 0)) + sum(tens))/sum(ifnull(tuh, 20))) as \"TU%\",
 printf("%.1f", avg(total_pts)) as PPG, 
 printf("%.2f", sum(bonus_pts)/sum(bonuses_heard)) as PPB
from team_games
LEFT JOIN tournaments on team_games.tournament_id = tournaments.tournament_id
 LEFT JOIN sets on team_games.set_id = sets.set_id
 LEFT JOIN sites on team_games.site_id = sites.site_id
 LEFT JOIN schools on team_games.school_id = schools.school_id
 LEFT JOIN teams on team_games.team_id = teams.team_id
 WHERE sites.circuit_slug = '{circuit_slug}'
 and schools.circuit_slug = '{circuit_slug}'
 GROUP BY 1, 2, 3, 4""")

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        schools_res = [dict(zip(keys, row)) for row in rows]
        for tournament in schools_res:
            if tournament['school_slug']:
                tournament['school_name'] = tournament['School']
                tournament['School'] = f"<a href = '../schools/{tournament['school_slug']}'>{tournament['School']}</a>"

                
        cur.execute(f"""
        SELECT sites.site, sites.lat, sites.lon, sites.school_id,
ifnull(cast(max(strftime('%Y', tournaments.date)) as int), 2000) as last_host
from sites
 LEFT JOIN tournaments on sites.site_id = tournaments.site_id
 WHERE sites.circuit_slug = '{circuit_slug}'
 and sites.lat is not null
 GROUP BY 1""")

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        sites_res = pd.DataFrame([dict(zip(keys, row)) for row in rows]).merge(pd.DataFrame(schools_res)[['school_id', 'last_active']], on = 'school_id')
        sites_res['last_active'] = np.fmax(sites_res['last_active'], sites_res['last_host'])
        print(sites_res[['site', 'last_host', 'last_active', 'lat', 'lon']])
        sites_res = sites_res.to_dict('records')

        cur.execute(f"""
       SELECT 
date as Date,
coalesce(\"set\" || ' at ' || site, '') as Tournament, team_games.tournament_id,
team as Champion
from team_games
left join teams on team_games.team_id = teams.team_id
left join tournaments on team_games.tournament_id = tournaments.tournament_id
LEFT JOIN sets on tournaments.set_id = sets.set_id
LEFT JOIN sites on tournaments.site_id = sites.site_id
LEFT JOIN tournament_results on team_games.tournament_id = tournament_results.tournament_id
and team_games.team_id = tournament_results.team_id
where rank = 1
and sites.circuit_slug = '{circuit_slug}'
GROUP BY 1, 2""")

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        res = [dict(zip(keys, row)) for row in rows]
        for en in res:
                en['Champion'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Champion'])}'>{en['Champion']}</a>"
        champs_res = pd.DataFrame(res)

        cur.execute(f"""
       SELECT sites.circuit as Circuit,
sets.year as Year, tournaments.date as Date,
\"set\" as \"Set\", set_slug,
\"set\" || ' at ' || site as Tournament, team_games.tournament_id,
site as Site, count(distinct team_id) as Teams,
count(distinct(team_games.school_id)) as Schools,
round(sum(powers)/sum(ifnull(tuh, 20)), 3) as pct_power,
round((sum(ifnull(powers,0))+sum(tens))/sum(ifnull(tuh, 20)/2), 3) as pct_conv,
round(sum(bonus_pts)/sum(bonuses_heard), 2) as PPB
from team_games
LEFT JOIN tournaments on team_games.tournament_id = tournaments.tournament_id
 LEFT JOIN sets on tournaments.set_id = sets.set_id
 LEFT JOIN sites on tournaments.site_id = sites.site_id
 WHERE sites.circuit_slug = '{circuit_slug}'
 GROUP BY 1,2,3,4
 ORDER by Date desc""")

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        res = [dict(zip(keys, row)) for row in rows]
        for en in res:
                en['Set'] = f"<a href = '../sets/{en['set_slug']}'>{en['Set']}</a>"
                en['Site'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Site']}</a>"

        tournaments_res = pd.DataFrame(res).merge(
            champs_res, on=['Date', 'tournament_id']
        ).fillna('').to_dict('records')

        return {
            'Schools': schools_res,
            'Sites': sites_res,
            'Tournaments': tournaments_res
        }
