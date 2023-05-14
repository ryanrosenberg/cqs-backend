from flask_restful import Resource

import sqlite3 as sq
import utils


class TeamList(Resource):
    def get(self):
        con = sq.connect("stats.db")
        cur = con.cursor()
        cur.execute("""SELECT slug from 
        team_games
        left join teams on team_games.team_id = teams.team_id
        left join schools on teams.school_id = schools.school_id
                    where slug is not null
                    and school_name is not null""")

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        res = [dict(zip(keys, row)) for row in rows]

        return res


class Team(Resource):
    def get(self, team_slug):
        con = sq.connect("stats.db")
        cur = con.cursor()

        cur.execute(f"""SELECT school_name as School, schools.circuit as Circuit,
           sets.year as Year,
           count(distinct player_games.tournament_id) as Tmnts,
           max(tournament_teams) as Teams,
           count(distinct player_id) as Players,
           nats_rank as \"ACF Nats\", nats_id, ict_id,
           ict_rank as \"DI ICT\" from 
           player_games
           LEFT JOIN schools on player_games.school_id = schools.school_id
           LEFT JOIN teams on player_games.team_id = teams.team_id
           LEFT JOIN (SELECT tournament_id, count(distinct team_id) as tournament_teams from team_games LEFT JOIN schools on team_games.school_id = schools.school_id WHERE slug = '{team_slug}' GROUP BY 1) tournament_teams on player_games.tournament_id = tournament_teams.tournament_id
           LEFT JOIN tournaments on player_games.tournament_id = tournaments.tournament_id
           LEFT JOIN sets on tournaments.set_id = sets.set_id
           LEFT JOIN sites on tournaments.site_id = sites.site_id
           LEFT JOIN (SELECT sets.year as Year, rank as nats_rank, tournament_results.tournament_id as nats_id
           FROM tournament_results
           LEFT JOIN tournaments on tournament_results.tournament_id = tournaments.tournament_id
           LEFT JOIN sets on tournaments.set_id = sets.set_id
           LEFT JOIN teams on tournament_results.team_id = teams.team_id
           LEFT JOIN schools on teams.school_id = schools.school_id
           WHERE \"set\" = 'ACF Nationals'
           and slug = '{team_slug}') nats on sets.year = nats.year
           LEFT JOIN (SELECT sets.year as Year, rank as ict_rank, tournament_results.tournament_id as ict_id
           FROM tournament_results
           LEFT JOIN tournaments on tournament_results.tournament_id = tournaments.tournament_id
           LEFT JOIN sets on tournaments.set_id = sets.set_id
           LEFT JOIN teams on tournament_results.team_id = teams.team_id
           LEFT JOIN schools on teams.school_id = schools.school_id
           WHERE \"set\" = 'DI ICT'
           and slug = '{team_slug}') ict on sets.year = ict.year
           WHERE slug = '{team_slug}'
           GROUP BY 1, 2, 3
           ORDER BY 3 desc""")

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        summary_res = [dict(zip(keys, row)) for row in rows]
        for tournament in summary_res:
            tournament['Year'] = f"<a href = '../seasons/{tournament['Year']}'>{tournament['Year']}</a>"


        cur.execute(f"""SELECT 
           team as Team,
           count(distinct team_games.tournament_id) as Tmnts,
           count(result) as GP,
           sum(case result when 1 then 1 else 0 end) || '-' ||
           sum(case result when 0 then 1 else 0 end) as \"W-L\",
           printf("%.3f", avg(result)) as \"Win%\",
           sum(ifnull(tuh, 20)) as TUH,
           sum(powers) as \"15\", sum(tens) as \"10\", sum(negs) as \"-5\",
           printf("%.1f", sum(powers)/count(result)) as \"15/G\",
           printf("%.1f", sum(tens)/count(result)) as \"10/G\",
           printf("%.1f", sum(negs)/count(result)) as \"-5/G\",
           printf("%.3f", (sum(ifnull(powers, 0)) + sum(tens))/sum(ifnull(tuh, 20))) as \"TU%\",
           printf("%.1f", avg(total_pts)) as PPG, 
           printf("%.2f", sum(bonus_pts)/sum(bonuses_heard)) as PPB from 
           team_games
           LEFT JOIN schools on team_games.school_id = schools.school_id
           LEFT JOIN teams on team_games.team_id = teams.team_id
           LEFT JOIN tournaments on team_games.tournament_id = tournaments.tournament_id
           LEFT JOIN sets on tournaments.set_id = sets.set_id
           LEFT JOIN sites on tournaments.site_id = sites.site_id
           WHERE slug = '{team_slug}'
           GROUP BY 1
           ORDER BY 3 desc""")

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        teams_res = [dict(zip(keys, row)) for row in rows]

        cur.execute(f"""
           SELECT 
           tournaments.date || ': ' || tournaments.tournament_name as Tournament, team_games.tournament_id,
            sets.year as Year,
           sets.year || ' ' || \"set\" || ' ' || site as tournament_name,
           date as Date,
           teams.team as Team,
           players as Players,
           cast(rank as int) || '/' || cast(num_teams as int) as Finish,
           count(result) as GP,
           sum(case result when 1 then 1 else 0 end) || '-' ||
           sum(case result when 0 then 1 else 0 end) as \"W-L\",
           sum(ifnull(tuh, 20)) as TUH,
           sum(powers) as \"15\", sum(tens) as \"10\", sum(negs) as \"-5\",
           printf("%.1f", sum(powers)/count(result)) as \"15/G\",
           printf("%.1f", sum(tens)/count(result)) as \"10/G\",
           printf("%.1f", sum(negs)/count(result)) as \"-5/G\",
           printf("%.3f", (sum(ifnull(powers, 0)) + sum(tens))/sum(ifnull(tuh, 20))) as \"TU%\",
           printf("%.1f", avg(total_pts)) as PPG, 
           printf("%.2f", sum(bonus_pts)/sum(bonuses_heard)) as PPB,
           printf("%.2f", a_value) as \"A-Value\" 
           from team_games
           LEFT JOIN schools on team_games.school_id = schools.school_id
           LEFT JOIN teams on team_games.team_id = teams.team_id
           LEFT JOIN tournaments on team_games.tournament_id = tournaments.tournament_id
           LEFT JOIN tournament_results on team_games.tournament_id = tournament_results.tournament_id
           and team_games.team_id = tournament_results.team_id
           LEFT JOIN sets on tournaments.set_id = sets.set_id
           LEFT JOIN sites on tournaments.site_id = sites.site_id
           LEFT JOIN (SELECT tournament_id, team_id, 
           group_concat(distinct ' ' || coalesce(fname|| ' ' || lname, player_games.player)) as players
            from player_games
            LEFT JOIN schools on player_games.school_id = schools.school_id
            LEFT JOIN players on player_games.player_id = players.player_id
            LEFT JOIN people on players.person_id = people.person_id
            WHERE schools.slug = '{team_slug}'
            GROUP BY 1, 2) player_games
           on team_games.tournament_id = player_games.tournament_id
           and team_games.team_id = player_games.team_id
           WHERE schools.slug = '{team_slug}'
           GROUP BY 1, 2, 3, 4, 5, 6
           ORDER BY date desc""")

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        tournament_res = [dict(zip(keys, row)) for row in rows]
        for tournament in tournament_res:
            if tournament['tournament_id']:
                tournament['Team'] = f"<a href = '../tournaments/{str(tournament['tournament_id']).replace('.0', '')}#{utils.slug(tournament['Team'])}'>{tournament['Team']}</a>"
                tournament['Tournament'] = f"<a href = '../tournaments/{str(tournament['tournament_id']).replace('.0', '')}'>{tournament['Tournament']}</a>"

        cur.execute(f"""
        SELECT 
           fname || ' ' || lname as Player, 
           schools.slug, people.slug as person_slug,
           strftime('%Y', min(date)) || '-' || strftime('%Y', max(date)) as Yrs,
           count(distinct player_games.tournament_id) as Tmnts,
           count(tens) as GP,
           sum(ifnull(tuh, 20)) as TUH,
           sum(ifnull(powers, 0)) as \"15\", sum(tens) as \"10\", sum(negs) as \"-5\",
           printf("%.1f", sum(ifnull(powers, 0))/count(tens)) as \"15/G\",
           printf("%.1f", sum(tens)/count(tens)) as \"10/G\",
           printf("%.1f", sum(negs)/count(tens)) as \"-5/G\",
           printf("%.3f", (sum(ifnull(powers, 0)) + sum(tens))/sum(ifnull(tuh, 20))) as \"TU%\",
           printf("%.2f", avg(pts)) as PPG from 
           player_games
           LEFT JOIN schools on player_games.school_id = schools.school_id
           LEFT JOIN tournaments on player_games.tournament_id = tournaments.tournament_id
           INNER JOIN players on player_games.player_id = players.player_id
           LEFT JOIN people on players.person_id = people.person_id
           WHERE schools.slug = '{team_slug}'
           GROUP BY 1, 2, 3
        """)

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        player_res = [dict(zip(keys, row)) for row in rows]
        for player in player_res:
            if player['person_slug']:
                player['Player'] = f"<a href = '../players/{player['person_slug']}'>{player['Player']}</a>"

        cur.execute(f"""
        SELECT 
           tournaments.tournament_id,
           sets.year as Year,
           date as Date,
           \"set\" as \"Set\",
           count(distinct teams.team) as Teams
           from team_games
           LEFT JOIN teams on team_games.team_id = teams.team_id
           LEFT JOIN tournaments on team_games.tournament_id = tournaments.tournament_id
           LEFT JOIN tournament_results on team_games.tournament_id = tournament_results.tournament_id
           and team_games.team_id = tournament_results.team_id
           LEFT JOIN sets on tournaments.set_id = sets.set_id
           LEFT JOIN sites on tournaments.site_id = sites.site_id
           LEFT JOIN schools on sites.school_id = schools.school_id
           WHERE schools.slug = '{team_slug}'
           GROUP BY 1, 2, 3, 4
           ORDER BY date desc
        """)

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        hosting_res = [dict(zip(keys, row)) for row in rows]
        for en in hosting_res:
            en['Teams'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Teams']}</a>"

        res = {
            'Summary': summary_res,
            'Teams': teams_res,
            'Players': player_res,
            'Tournaments': tournament_res,
            'Hosting': hosting_res
        }

        return res


class TeamsThisYear(Resource):
    def get(self):
        con = sq.connect("stats.db")
        cur = con.cursor()

        cur.execute("""
        SELECT 
team as Team,
school as School, slug,
count(distinct team_games.tournament_id) as Ts,
count(result) as GP,
sum(case result when 1 then 1 else 0 end) as W,
sum(case result when 0 then 1 else 0 end) as L,
printf("%.3f", avg(result)) as \"Win%\",
sum(ifnull(tuh, 20)) as TUH,
printf("%.2f", sum(powers)/count(result)) as \"15/G\",
printf("%.2f", sum(tens)/count(result)) as \"10/G\",
printf("%.2f", sum(negs)/count(result)) as \"-5/G\",
printf("%.3f", (sum(ifnull(powers, 0)) + sum(tens))/sum(ifnull(tuh, 20))) as \"TU%\",
printf("%.2f", avg(total_pts)) as PPG, 
printf("%.2f", sum(bonus_pts)/sum(bonuses_heard)) as PPB
from team_games
left join teams on team_games.team_id = teams.team_id
left join schools on teams.school_id = schools.school_id
left join tournaments on team_games.tournament_id = tournaments.tournament_id
left join sets on tournaments.set_id = sets.set_id
where sets.year = '22-23'
and teams.school_id is not null
GROUP BY 1
ORDER BY GP desc""")

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        res = [dict(zip(keys, row)) for row in rows]
        for team in res:
            if team['slug']:
                team['School'] = f"<a href = '../schools/{team['slug']}'>{team['School']}</a>"

        return res
