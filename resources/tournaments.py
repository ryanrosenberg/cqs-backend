from flask_restful import Resource

import sqlite3 as sq
import pandas as pd
import utils


class RecentTournaments(Resource):
    def get(self):
        con = sq.connect("stats.db")
        cur = con.cursor()

        cur.execute(""" SELECT tournaments.*, results.Champion FROM (SELECT 
                    date as Date,
                    sets.year, \"set\" as \"Set\", site as Site,
                    \"set\" || ' at ' || site as Tournament, tournaments.tournament_id,
                    count(distinct team_games.team_id) as Teams
                    from team_games
                    left join teams on team_games.team_id = teams.team_id
                    left join tournaments on team_games.tournament_id = tournaments.tournament_id
                    LEFT JOIN sets on tournaments.set_id = sets.set_id
                    LEFT JOIN sites on tournaments.site_id = sites.site_id
                    GROUP BY 1, 2, 3, 4, 5, 6
                    ORDER BY Date desc
                    LIMIT 10) tournaments
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
                        where rank = 1
                        GROUP BY 1, 2
                        ORDER BY Date desc
                        LIMIT 10
                    ) results
                    on tournaments.Tournament = results.Tournament
                    """
                    )

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        res = [dict(zip(keys, row)) for row in rows]
        for tournament in res:
            tournament['Tournament'] = f"<a href = 'tournaments/{str(tournament['tournament_id']).replace('.0', '')}'>{tournament['Tournament']}</a>"

        return res


class TournamentList(Resource):
    def get(self):
        con = sq.connect("stats.db")
        cur = con.cursor()

        cur.execute("SELECT tournament_id from tournaments")
        res = cur.fetchall()
        paths = [
            {
                'slug': str(player[0]).replace('.0', '')
            }
            for player in res
        ]

        return paths


class Tournament(Resource):
    def get(self, tournament_id):
        con = sq.connect("stats.db")
        cur = con.cursor()

        cur.execute(f"""
        SELECT 
           date,  \"set\" || ' at ' || site as tournament_name
           from tournaments 
           LEFT JOIN sets on tournaments.set_id = sets.set_id
           LEFT JOIN sites on tournaments.site_id = sites.site_id
           WHERE tournaments.tournament_id = {tournament_id}
        """)

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        summary_res = [dict(zip(keys, row)) for row in rows]

        cur.execute(f"""SELECT 
           rank as Rank,
           team as Team,
           schools.school_name as School, slug, bracket,
           count(result) as GP,
           sum(case result when 1 then 1 else 0 end) || '-' ||
           sum(case result when 0 then 1 else 0 end) as \"W-L\",
           sum(ifnull(tuh, 20)) as TUH,
           sum(powers) as \"15\", sum(tens) as \"10\", sum(negs) as \"-5\",
           printf("%.2f", sum(powers)/count(result)) as \"15/G\",
           printf("%.2f", sum(tens)/count(result)) as \"10/G\",
           printf("%.2f", sum(negs)/count(result)) as \"-5/G\",
           printf("%.3f", (sum(ifnull(powers, 0)) + sum(tens))/sum(ifnull(tuh, 20))) as \"TU%\",
           printf("%.1f", avg(total_pts)) as PPG, 
           printf("%.2f", sum(bonus_pts)/sum(bonuses_heard)) as PPB,
           printf("%.1f", a_value) as \"A-Value\" 
           from team_games
           LEFT JOIN teams on team_games.team_id = teams.team_id
           LEFT JOIN schools on teams.school_id = schools.school_id
           LEFT JOIN tournaments on team_games.tournament_id = tournaments.tournament_id
           LEFT JOIN tournament_results on team_games.tournament_id = tournament_results.tournament_id
           and team_games.team_id = tournament_results.team_id
           LEFT JOIN sets on tournaments.set_id = sets.set_id
           LEFT JOIN sites on tournaments.site_id = sites.site_id
           WHERE team_games.tournament_id = {tournament_id}
           GROUP BY 1, 2, 3, 4, 5
        """)

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        standings_res = [dict(zip(keys, row)) for row in rows]
        for team in standings_res:
            if team['slug']:
                team['School'] = f"<a href = '../schools/{team['slug']}'>{team['School']}</a>"
            team['Team'] = f"<a href = '#{utils.slug(team['Team'])}'>{team['Team']}</a>"

        cur.execute(f"""
        SELECT *, printf("%.2f", rawPPG) as PPG from (
        SELECT
           coalesce(fname|| ' ' || lname, player_games.player) as Player,
           coalesce(fname|| ' ' || lname, player_games.player) as raw_player,
           slug,
           team as Team,
           count(tens) as GP,
           sum(ifnull(tuh, 20)) as TUH,
           sum(powers) as \"15\", 
           sum(tens) as \"10\", 
           sum(negs) as \"-5\",
           printf("%.2f", sum(powers)/count(tens)) as \"15/G\",
           printf("%.2f", sum(tens)/count(tens)) as \"10/G\",
           printf("%.2f", sum(negs)/count(tens)) as \"-5/G\",
           printf("%.2f", sum(powers)/sum(negs)) as \"P/N\",
           printf("%.2f", (sum(ifnull(powers, 0)) + sum(tens))/sum(negs)) as \"G/N\",
           printf("%.3f", (sum(ifnull(powers, 0)) + sum(tens))/sum(ifnull(tuh, 20))) as \"TU%\",
           avg(pts) as rawPPG from
           player_games
           LEFT JOIN teams on player_games.team_id = teams.team_id
           LEFT JOIN tournaments on player_games.tournament_id = tournaments.tournament_id
           LEFT JOIN sets on tournaments.set_id = sets.set_id
           LEFT JOIN sites on tournaments.site_id = sites.site_id
           LEFT JOIN players on player_games.player_id = players.player_id
           LEFT JOIN people on players.person_id = people.person_id
           WHERE player_games.tournament_id = {tournament_id}
           GROUP BY 1, 2, 3, 4
           ORDER BY rawPPG desc)
        """)

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        players_res = [dict(zip(keys, row)) for row in rows]
        for team in players_res:
            team['Player'] = f"<a href = '#{utils.slug(team['Player']) + '-' + utils.slug(team['Team'])}'>{team['Player']}</a>"
            team['Team'] = f"<a href = '#{utils.slug(team['Team'])}'>{team['Team']}</a>"

        cur.execute(f"""
        SELECT
           CAST(REPLACE(round, 'Round ', '') as int) as Round,
           team as Team,
           game_num, game_id,
           opponent as Opponent,
           case result when 1 then 'W' when 0 then 'L' else 'T' end as Result,
           total_pts as PF, opp_pts as PA, powers as \"15\", tens as \"10\",
           negs as \"-5\", ifnull(tuh, 20) as TUH,
           printf("%.2f", total_pts/ifnull(tuh, 20)) as PPTUH, 
           bonuses_heard as BHrd, bonus_pts as BPts,
           printf("%.2f", bonus_pts/bonuses_heard) as PPB
           from team_games
           LEFT JOIN teams on team_games.team_id = teams.team_id
           LEFT JOIN (select team_id, team as opponent_team from teams) a on team_games.opponent_id = a.team_id
           where team_games.tournament_id = {tournament_id}
           order by Team, Round
        """)

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        team_detail_team_res = [dict(zip(keys, row)) for row in rows]
        for team in team_detail_team_res:
            team['Result'] = f"<a href = '../games/{utils.string(team['game_id'])}'>{team['Result']}</a>"
            if team['Opponent']:
                team['Opponent'] = f"<a href = '#{utils.slug(team['Opponent'])}'>{team['Opponent']}</a>"


        cur.execute(f"""
        SELECT 
           coalesce(fname|| ' ' || lname, player_games.player) as Player, 
           team as Team,
           count(tens) as GP,
           sum(ifnull(tuh, 20)) as TUH,
           sum(powers) as \"15\", sum(tens) as \"10\", sum(negs) as \"-5\",
           printf("%.2f", sum(powers)/count(tens)) as \"15/G\",
           printf("%.2f", sum(tens)/count(tens)) as \"10/G\",
           printf("%.2f", sum(negs)/count(tens)) as \"-5/G\",
           printf("%.2f", sum(powers)/sum(negs)) as \"P/N\",
           printf("%.2f", (sum(ifnull(powers, 0)) + sum(tens))/sum(negs)) as \"G/N\",
           printf("%.3f", (sum(ifnull(powers, 0)) + sum(tens))/sum(ifnull(tuh, 20))) as \"TU%\",
           sum(pts) as Pts,
           printf("%.2f", avg(pts)) as PPG from 
           player_games
           LEFT JOIN teams on player_games.team_id = teams.team_id
           LEFT JOIN tournaments on player_games.tournament_id = tournaments.tournament_id
           LEFT JOIN players on player_games.player_id = players.player_id
           LEFT JOIN people on players.person_id = people.person_id
           where player_games.tournament_id = {tournament_id}
           GROUP BY 1, 2
           order by Team, Player
        """)

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        team_detail_player_res = [dict(zip(keys, row)) for row in rows]
        for team in team_detail_player_res:
            team['Player'] = f"<a href = '#{utils.slug(team['Player']) + '-' + utils.slug(team['Team'])}'>{team['Player']}</a>"

        cur.execute(f"""SELECT
coalesce(fname|| ' ' || lname, player_games.player) as player, team,
CAST(REPLACE(games.round, 'Round ', '') as int) as Round,
opponent_team as Opponent,
player_games.game_num, player_games.game_id,
case result when 1 then 'W' when 0 then 'L' else 'T' end as Result,
ifnull(player_games.tuh, 20) as TUH,
player_games.powers as \"15\", player_games.tens as \"10\", player_games.negs as \"-5\", pts as Pts
from player_games
LEFT JOIN team_games on player_games.game_id = team_games.game_id
and player_games.team_id = team_games.team_id
LEFT JOIN (select team_id, team as opponent_team from teams) a on team_games.opponent_id = a.team_id
            LEFT JOIN players on player_games.player_id = players.player_id
            LEFT JOIN people on players.person_id = people.person_id
            LEFT JOIN teams on player_games.team_id = teams.team_id
            left join games on player_games.game_id = games.game_id
WHERE player_games.tournament_id = {tournament_id}
           order by team, player, Round
        """)

        rows = cur.fetchall()
        keys = [k[0] for k in cur.description]
        player_detail_res = [dict(zip(keys, row)) for row in rows]
        for team in player_detail_res:
            team['Result'] = f"<a href = '../games/{utils.string(team['game_id'])}'>{team['Result']}</a>"
            if team['Opponent']:
                team['Opponent'] = f"<a href = '#{utils.slug(team['Opponent'])}'>{team['Opponent']}</a>"

        all = {
            'Summary': summary_res,
            'Standings': standings_res,
            'Players': players_res,
            'Team Detail Teams': team_detail_team_res,
            'Team Detail Players': team_detail_player_res,
            'Player Detail': player_detail_res
        }

        return all
