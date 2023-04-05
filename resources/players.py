from flask_restful import Resource

import sqlite3 as sq


class PlayerList(Resource):
    def get(self):
        con = sq.connect("stats.db")
        cur = con.cursor()
        cur.execute("SELECT slug, player from people where slug is not null and schools is not null")
        res = cur.fetchall()
        paths = [
            {
                'slug': player[0]
            }
            for player in res
        ]

        return paths


class Player(Resource):
    def get(self, player_slug):
        con = sq.connect("stats.db")
        cur = con.cursor()
        cur.execute(f"""
        SELECT
           sets.year as Year,
           people.player as Player,
           school_name as School, schools.slug as school_slug,
           count(distinct tournaments.tournament_id) as Ts,
           count(tens) as GP,
           sum(ifnull(tuh, 20)) as TUH,
           sum(ifnull(powers, 0)) as \"15\", sum(tens) as \"10\", sum(negs) as \"-5\",
           printf("%.1f", avg(ifnull(powers, 0))) as \"15/G\",
           printf("%.1f", avg(tens)) as \"10/G\",
           printf("%.1f", avg(negs)) as \"-5/G\",
           printf("%.2f", (sum(ifnull(powers, 0)) + sum(tens))/count(negs)) as \"G/N\",
           printf("%.2f", avg(pts)) as PPG from
           player_games
           LEFT JOIN schools on player_games.school_id = schools.school_id
           LEFT JOIN teams on player_games.team_id = teams.team_id
           LEFT JOIN tournaments on player_games.tournament_id = tournaments.tournament_id
           LEFT JOIN sets on tournaments.set_id = sets.set_id
           LEFT JOIN sites on tournaments.site_id = sites.site_id
           INNER JOIN players on player_games.player_id = players.player_id
           LEFT JOIN people on players.person_id = people.person_id
            where people.slug = '{player_slug}'
           and teams.school is not null
           GROUP BY 1, 2, 3, 4
           ORDER BY 1""")

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        years_res = [dict(zip(keys, row)) for row in rows]
        for tournament in years_res:
            if tournament['school_slug']:
                tournament['School'] = f"<a href = '../schools/{tournament['school_slug']}'>{tournament['School']}</a>"

        cur.execute(f"""
       SELECT
           sets.year as Year,
           date as Date,
           \"set\" as \"Set\", tournaments.tournament_id,
           site as Site,
           teams.school as School, schools.slug as school_slug, sets.set_slug as set_slug,
           coalesce(teams.team, school_name) as Team,
           rank || '/' || CAST(num_teams as int) as Finish,
           count(tens) as GP,
           sum(ifnull(tuh, 20)) as TUH,
           sum(powers) as \"15\", sum(tens) as \"10\", sum(negs) as \"-5\",
           printf("%.1f", sum(powers)/count(tens)) as \"15/G\",
           printf("%.1f", sum(tens)/count(tens)) as \"10/G\",
           printf("%.1f", sum(negs)/count(tens)) as \"-5/G\",
           printf("%.2f", sum(powers)/sum(negs)) as \"P/N\",
           printf("%.2f", (sum(ifnull(powers, 0)) + sum(tens))/sum(negs)) as \"G/N\",
           printf("%.2f", avg(pts)) as PPG from
           player_games
           LEFT JOIN schools on player_games.school_id = schools.school_id
           LEFT JOIN teams on player_games.team_id = teams.team_id
           LEFT JOIN tournaments on player_games.tournament_id = tournaments.tournament_id
           LEFT JOIN tournament_results on player_games.tournament_id = tournament_results.tournament_id
           and player_games.team_id = tournament_results.team_id
           LEFT JOIN sets on tournaments.set_id = sets.set_id
           LEFT JOIN sites on tournaments.site_id = sites.site_id
           INNER JOIN players on player_games.player_id = players.player_id
           LEFT JOIN people on players.person_id = people.person_id
           WHERE people.slug = '{player_slug}'
           GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
           ORDER BY 2""")

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        tournaments_res = [dict(zip(keys, row)) for row in rows]
        for tournament in tournaments_res:
            if tournament['school_slug']:
                tournament['School'] = f"<a href = '../schools/{tournament['school_slug']}'>{tournament['School']}</a>"
            if tournament['set_slug']:
                tournament['Set'] = f"<a href = '../sets/{tournament['set_slug']}'>{tournament['Set']}</a>"
            tournament['Site'] = f"<a href = '../tournaments/{str(tournament['tournament_id']).replace('.0', '')}'>{tournament['Site']}</a>"

        cur.execute(f"""
       SELECT 
year as Year, \"set\" as \"Set\", sets.set_slug, group_concat(subcategory, ', ') as Categories
from editors
LEFT JOIN sets on editors.set_id = sets.set_id
LEFT JOIN people on editors.person_id = people.person_id
WHERE slug = '{player_slug}'
GROUP BY 1, 2, 3""")

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        editing_res = [dict(zip(keys, row)) for row in rows]
        for set in editing_res:
            if set['set_slug']:
                set['Set'] = f"<a href = '../sets/{set['set_slug']}'>{set['Set']}</a>"

        return {'Years': years_res, 'Tournaments': tournaments_res, 'Editing': editing_res}
