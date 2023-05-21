import firebase_admin
from firebase_admin import firestore
import sqlite3 as sq
import pandas as pd
import numpy as np
import utils
import pickle

# Application Default credentials are automatically created.
app = firebase_admin.initialize_app()
db = firestore.client()


def slug(str):
    new_str = str.lower().replace(" ", "-")
    return new_str


def string(int):
    new_str = str(int).replace(".0", "")
    return new_str


con = sq.connect("stats.db")
cur = con.cursor()

# Circuits
# cur.execute(
#     """SELECT distinct circuit_slug from schools
#     where circuit is not null
#     and circuit <> 'National'
#     and circuit <> 'Asia'"""
# )
# circuit_slugs = [string(r[0]) for r in cur.fetchall() if r[0] is not None]

# for circuit_slug in circuit_slugs:
#     print(circuit_slug)
#     cur.execute(
#         f"""
#         SELECT
# school_name as School, slug as school_slug, schools.lat, schools.lon, schools.school_id,
# min(strftime('%Y', tournaments.date)) || '-' || max(strftime('%Y', tournaments.date)) as Yrs,
# ifnull(cast(max(strftime('%Y', tournaments.date)) as int), 2000) as last_active,
# count(distinct tournaments.tournament_id) as Ts,
# count(game_id) as GP,
#  sum(case result when 1 then 1 else 0 end) || '-' ||
#  sum(case result when 0 then 1 else 0 end) as \"W-L\",
#  printf("%.3f", avg(result)) as \"Win%\",
#  sum(ifnull(tuh, 20)) as TUH,
#  sum(powers) as \"15\", sum(tens) as \"10\", sum(negs) as \"-5\",
#  printf("%.2f", sum(powers)/count(result)) as \"15/G\",
#  printf("%.2f", sum(tens)/count(result)) as \"10/G\",
#  printf("%.2f", sum(negs)/count(result)) as \"-5/G\",
#  printf("%.3f", (sum(ifnull(powers, 0)) + sum(tens))/sum(ifnull(tuh, 20))) as \"TU%\",
#  printf("%.1f", avg(total_pts)) as PPG,
#  printf("%.2f", sum(bonus_pts)/sum(bonuses_heard)) as PPB
# from team_games
# LEFT JOIN tournaments on team_games.tournament_id = tournaments.tournament_id
#  LEFT JOIN sets on team_games.set_id = sets.set_id
#  LEFT JOIN sites on team_games.site_id = sites.site_id
#  LEFT JOIN schools on team_games.school_id = schools.school_id
#  LEFT JOIN teams on team_games.team_id = teams.team_id
#  WHERE sites.circuit_slug = '{circuit_slug}'
#  and schools.circuit_slug = '{circuit_slug}'
#  GROUP BY 1, 2, 3, 4"""
#     )

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     schools_res = [dict(zip(keys, row)) for row in rows]
#     for tournament in schools_res:
#         if tournament["school_slug"]:
#             tournament["school_name"] = tournament["School"]
#             tournament[
#                 "School"
#             ] = f"<a href = '../schools/{tournament['school_slug']}'>{tournament['School']}</a>"

#     cur.execute(
#         f"""
#     SELECT sites.site, sites.lat, sites.lon, sites.school_id,
# ifnull(cast(max(strftime('%Y', tournaments.date)) as int), 2000) as last_host
# from sites
# LEFT JOIN tournaments on sites.site_id = tournaments.site_id
# WHERE sites.circuit_slug = '{circuit_slug}'
# and sites.lat is not null
# GROUP BY 1"""
#     )

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     sites_res = pd.DataFrame([dict(zip(keys, row)) for row in rows]).merge(
#         pd.DataFrame(schools_res)[["school_id", "last_active"]], on="school_id"
#     )
#     sites_res["last_active"] = np.fmax(sites_res["last_active"], sites_res["last_host"])
#     sites_res = sites_res.to_dict("records")

#     cur.execute(
#         f"""
#     SELECT
# date as Date,
# coalesce(\"set\" || ' at ' || site, '') as Tournament, team_games.tournament_id,
# teams.team as Champion
# from team_games
# left join teams on team_games.team_id = teams.team_id
# left join tournaments on team_games.tournament_id = tournaments.tournament_id
# LEFT JOIN sets on tournaments.set_id = sets.set_id
# LEFT JOIN sites on tournaments.site_id = sites.site_id
# LEFT JOIN tournament_results on team_games.tournament_id = tournament_results.tournament_id
# and team_games.team_id = tournament_results.team_id
# where rank = 1
# and sites.circuit_slug = '{circuit_slug}'
# GROUP BY 1, 2"""
#     )

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     res = [dict(zip(keys, row)) for row in rows]
#     for en in res:
#         en[
#             "Champion"
#         ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}/team-detail#{utils.slug(en['Champion'])}'>{en['Champion']}</a>"
#     champs_res = pd.DataFrame(res)

#     cur.execute(
#         f"""
#     SELECT sites.circuit as Circuit,
# sets.year as Year, tournaments.date as Date,
# \"set\" as \"Set\", set_slug,
# \"set\" || ' at ' || site as Tournament, team_games.tournament_id,
# site as Site, count(distinct team_id) as Teams,
# count(distinct(team_games.school_id)) as Schools,
# round(sum(powers)/sum(ifnull(tuh, 20)), 3) as pct_power,
# round((sum(ifnull(powers,0))+sum(tens))/sum(ifnull(tuh, 20)/2), 3) as pct_conv,
# round(sum(bonus_pts)/sum(bonuses_heard), 2) as PPB
# from team_games
# LEFT JOIN tournaments on team_games.tournament_id = tournaments.tournament_id
# LEFT JOIN sets on tournaments.set_id = sets.set_id
# LEFT JOIN sites on tournaments.site_id = sites.site_id
# WHERE sites.circuit_slug = '{circuit_slug}'
# GROUP BY 1,2,3,4
# ORDER by Date desc"""
#     )

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     res = [dict(zip(keys, row)) for row in rows]
#     for en in res:
#         en["Set"] = f"<a href = '../sets/{en['set_slug']}'>{en['Set']}</a>"
#         en[
#             "Site"
#         ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Site']}</a>"

#     tournaments_res = (
#         pd.DataFrame(res)
#         .merge(champs_res, on=["Date", "tournament_id"])
#         .fillna("")
#         .to_dict("records")
#     )

#     circuit_records = {}

#     cur.execute(
#         f"""
#     SELECT school_name as \"School\", 
# count(distinct tournaments.tournament_id) as Tournaments,
# sum(case result when 1 then 1 else 0 end) as Wins
# from team_games
# left join schools on team_games.school_id = schools.school_id
# left join sets on team_games.set_id = sets.set_id
# left join tournaments on team_games.tournament_id = tournaments.tournament_id
# left join sites on tournaments.site_id = sites.site_id
# where team_games.school_id is not null
# and (sets.difficulty <> 'easy' or sites.circuit in ('Asia', 'Central CC', 'Plains CC', 'Florida CC', 'Southeast CC'))
# and sites.circuit_slug = '{circuit_slug}'
# GROUP BY 1
# ORDER BY Wins desc
# LIMIT 10"""
#     )

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     res = [dict(zip(keys, row)) for row in rows]
#     for en in res:
#         en[
#             "School"
#         ] = f"<a href = '../schools/{utils.slug(en['School'])}'>{en['School']}</a>"
#     circuit_records["Most Wins"] = res

#     cur.execute(
#         f"""
#         SELECT * FROM(
#     SELECT team as \"Team\", 
# count(distinct tournaments.tournament_id) as Tournaments,
# round(avg(result), 3) as \"Win%\"
# from team_games
# left join schools on team_games.school_id = schools.school_id
# left join teams on team_games.team_id = teams.team_id
# left join sets on team_games.set_id = sets.set_id
# left join tournaments on team_games.tournament_id = tournaments.tournament_id
# left join sites on tournaments.site_id = sites.site_id
# where schools.open is null and schools.high_school is null
# and team_games.school_id is not null
# and (sets.difficulty <> 'easy' or sites.circuit in ('Asia', 'Central CC', 'Plains CC', 'Florida CC', 'Southeast CC'))
# and sites.circuit_slug = '{circuit_slug}'
# GROUP BY 1)
# WHERE Tournaments >=10
# ORDER BY \"Win%\" desc
# LIMIT 10"""
#     )

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     res = [dict(zip(keys, row)) for row in rows]
#     circuit_records["Highest Winning %"] = res

#     cur.execute(
#         f"""
#     SELECT school_name as \"School\", 
# count(distinct tournament_results.tournament_id) as Tournaments,
# sum(case rank when 1 then 1 else 0 end) as Wins
# from tournament_results
# left join teams on tournament_results.team_id = teams.team_id
# left join schools on teams.school_id = schools.school_id
# left join tournaments on tournament_results.tournament_id = tournaments.tournament_id
# left join sets on tournaments.set_id = sets.set_id
# left join sites on tournaments.site_id = sites.site_id
# where teams.school_id is not null
# and (sets.difficulty <> 'easy' or sites.circuit in ('Asia', 'Central CC', 'Plains CC', 'Florida CC', 'Southeast CC'))
# and sites.circuit_slug = '{circuit_slug}'
# GROUP BY 1
# ORDER BY 3 desc
# LIMIT 10"""
#     )

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     res = [dict(zip(keys, row)) for row in rows]
#     for en in res:
#         en[
#             "School"
#         ] = f"<a href = '../schools/{utils.slug(en['School'])}'>{en['School']}</a>"
#     circuit_records["Most Tournament Wins"] = res

#     cur.execute(
#         f"""
#     SELECT fname || ' ' || lname as Player, 
#     schools as Schools,
#     slug as player_slug,
# count(distinct tournament_id) as Ts,
# count(tournament_id) as GP,
# sum(pts) as Pts
# from player_games
# left join teams on player_games.team_id = teams.team_id
# left join sets on player_games.set_id = sets.set_id
# left join sites on player_games.site_id = sites.site_id
# LEFT JOIN players on player_games.player_id = players.player_id
# LEFT JOIN people on players.person_id = people.person_id
# where teams.school_id is not null
# and (sets.difficulty <> 'easy' or sites.circuit in ('Asia', 'Central CC', 'Plains CC', 'Florida CC', 'Southeast CC'))
# and sites.circuit_slug = '{circuit_slug}'
# and fname is not null
# GROUP BY 1, 2
# ORDER BY Pts desc
# LIMIT 10"""
#     )

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     res = [dict(zip(keys, row)) for row in rows]
#     for en in res:
#         en[
#             "Player"
#         ] = f"<a href = '../players/{en['player_slug']}'>{en['Player']}</a>"
#     circuit_records["Most Player Pts"] = res

#     cur.execute(
#         f"""
#     SELECT fname || ' ' || lname as Player, 
#     slug as player_slug,
# replace(group_concat(distinct teams.school), ',', ', ') as Schools,
# count(distinct tournament_id) as Ts
# from player_games
# left join teams on player_games.team_id = teams.team_id
# left join sets on player_games.set_id = sets.set_id
# LEFT JOIN players on player_games.player_id = players.player_id
# left join sites on player_games.site_id = sites.site_id
# LEFT JOIN people on players.person_id = people.person_id
# where teams.school_id is not null
# and (sets.difficulty <> 'easy' or sites.circuit in ('Asia', 'Central CC', 'Plains CC', 'Florida CC', 'Southeast CC'))
# and sites.circuit_slug = '{circuit_slug}'
# and fname is not null
# GROUP BY 1, 2
# ORDER BY Ts desc
# LIMIT 10"""
#     )

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     res = [dict(zip(keys, row)) for row in rows]
#     for en in res:
#         en[
#             "Player"
#         ] = f"<a href = '../players/{en['player_slug']}'>{en['Player']}</a>"
#     circuit_records["Most Tournaments Played"] = res

#     cur.execute(
#         f"""
#         SELECT * FROM(
#     SELECT fname || ' ' || lname as Player, 
#     slug as player_slug,
# replace(group_concat(distinct teams.school), ',', ', ') as Schools,
# count(player_games.game_id) as GP,
# round(avg(result), 3) as \"Win%\"
# from player_games
# left join (select game_id, team_id, result from team_games) results on player_games.team_id = results.team_id and player_games.game_id = results.game_id
# left join teams on player_games.team_id = teams.team_id
# left join sets on player_games.set_id = sets.set_id
# left join sites on player_games.site_id = sites.site_id
# LEFT JOIN players on player_games.player_id = players.player_id
# LEFT JOIN people on players.person_id = people.person_id
# where teams.school_id is not null
# and (sets.difficulty <> 'easy' or sites.circuit in ('Asia', 'Central CC', 'Plains CC', 'Florida CC', 'Southeast CC'))
# and sites.circuit_slug = '{circuit_slug}'
# and fname is not null
# GROUP BY 1, 2)
# WHERE GP >= 50
# ORDER BY \"Win%\" desc
# LIMIT 10"""
#     )

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     res = [dict(zip(keys, row)) for row in rows]
#     for en in res:
#         en[
#             "Player"
#         ] = f"<a href = '../players/{en['player_slug']}'>{en['Player']}</a>"
#     circuit_records["Highest Player Winning %"] = res

#     res = {
#         "Schools": schools_res,
#         "Sites": sites_res,
#         "Tournaments": tournaments_res,
#         "Records": circuit_records,
#     }

#     db.collection("circuits").document(circuit_slug).set(res)

# Players
# cur.execute("SELECT slug from people where slug is not null")
# player_slugs = [string(r[0]) for r in cur.fetchall() if r[0] is not None]
# player_slugs = ['adam-silverman', 'dylan-minarik', 'auroni-gupta', 'greg-peterson']
# for player_slug in player_slugs:
#     cur.execute(
#         f"""
#     SELECT
#         sets.year as Year,
#         people.player as Player,
#         school_name as School, schools.slug as school_slug,
#         count(distinct tournaments.tournament_id) as Ts,
#         count(tens) as GP,
#         sum(ifnull(tuh, 20)) as TUH,
#         sum(ifnull(powers, 0)) as \"15\", sum(tens) as \"10\", sum(negs) as \"-5\",
#         printf("%.1f", avg(ifnull(powers, 0))) as \"15/G\",
#         printf("%.1f", avg(tens)) as \"10/G\",
#         printf("%.1f", avg(negs)) as \"-5/G\",
#         printf("%.2f", (sum(ifnull(powers, 0)) + sum(tens))/count(negs)) as \"G/N\",
#         printf("%.2f", avg(pts)) as PPG from
#         player_games
#         LEFT JOIN schools on player_games.school_id = schools.school_id
#         LEFT JOIN teams on player_games.team_id = teams.team_id
#         LEFT JOIN tournaments on player_games.tournament_id = tournaments.tournament_id
#         LEFT JOIN sets on tournaments.set_id = sets.set_id
#         LEFT JOIN sites on tournaments.site_id = sites.site_id
#         INNER JOIN players on player_games.player_id = players.player_id
#         LEFT JOIN people on players.person_id = people.person_id
#         where people.slug = '{player_slug}'
#         and teams.school is not null
#         GROUP BY 1, 2, 3, 4
#         ORDER BY 1"""
#     )

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     years_res = [dict(zip(keys, row)) for row in rows]
#     for tournament in years_res:
#         if tournament["school_slug"]:
#             tournament[
#                 "School"
#             ] = f"<a href = '../schools/{tournament['school_slug']}'>{tournament['School']}</a>"

#     cur.execute(
#         f"""
#     SELECT
#         sets.year as Year,
#         date as Date,
#         people.player as Player,
#         \"set\" as \"Set\", tournaments.tournament_id,
#         site as Site,
#         teams.school as School, schools.slug as school_slug, sets.set_slug as set_slug,
#         coalesce(teams.team, school_name) as Team,
#         CAST(rank as int) || '/' || CAST(num_teams as int) as Finish,
#         W, L,
#         count(tens) as GP,
#         sum(ifnull(tuh, 20)) as TUH,
#         sum(powers) as \"15\", sum(tens) as \"10\", sum(negs) as \"-5\",
#         printf("%.1f", sum(powers)/count(tens)) as \"15/G\",
#         printf("%.1f", sum(tens)/count(tens)) as \"10/G\",
#         printf("%.1f", sum(negs)/count(tens)) as \"-5/G\",
#         printf("%.2f", sum(powers)/sum(negs)) as \"P/N\",
#         printf("%.2f", (sum(ifnull(powers, 0)) + sum(tens))/sum(negs)) as \"G/N\",
#         printf("%.2f", avg(pts)) as PPG from
#         player_games
#         LEFT JOIN schools on player_games.school_id = schools.school_id
#         LEFT JOIN teams on player_games.team_id = teams.team_id
#         LEFT JOIN tournaments on player_games.tournament_id = tournaments.tournament_id
#         LEFT JOIN tournament_results on player_games.tournament_id = tournament_results.tournament_id
#         and player_games.team_id = tournament_results.team_id
#         LEFT JOIN sets on tournaments.set_id = sets.set_id
#         LEFT JOIN sites on tournaments.site_id = sites.site_id
#         INNER JOIN players on player_games.player_id = players.player_id
#         LEFT JOIN people on players.person_id = people.person_id
#         WHERE people.slug = '{player_slug}'
#         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
#         ORDER BY 2"""
#     )

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     tournaments_res = [dict(zip(keys, row)) for row in rows]
#     for tournament in tournaments_res:
#         if tournament["school_slug"]:
#             tournament[
#                 "School"
#             ] = f"<a href = '../schools/{tournament['school_slug']}'>{tournament['School']}</a>"
#         if tournament["set_slug"]:
#             tournament[
#                 "Set"
#             ] = f"<a href = '../sets/{tournament['set_slug']}'>{tournament['Set']}</a>"
#         tournament[
#             "Site"
#         ] = f"<a href = '../tournaments/{str(tournament['tournament_id']).replace('.0', '')}'>{tournament['Site']}</a>"

#     cur.execute(
#         f"""
#     SELECT
# year as Year, \"set\" as \"Set\", sets.set_slug, group_concat(subcategory, ', ') as Categories
# from editors
# LEFT JOIN sets on editors.set_id = sets.set_id
# LEFT JOIN people on editors.person_id = people.person_id
# WHERE slug = '{player_slug}'
# GROUP BY 1, 2, 3"""
#     )

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     editing_res = [dict(zip(keys, row)) for row in rows]
#     for set in editing_res:
#         if set["set_slug"]:
#             set["Set"] = f"<a href = '../sets/{set['set_slug']}'>{set['Set']}</a>"

#     res = {
#         "Years": years_res,
#         "Tournaments": tournaments_res,
#         # "Editing": editing_res
#         }
#     db.collection("players").document(player_slug).update(res)
#     print(player_slug)


## Records
if False:
    records = []
    cur.execute(
        f"""
            SELECT school_name as \"School\", slug,
    count(distinct tournament_id) as Tournaments,
    sum(case result when 1 then 1 else 0 end) as Wins
    from team_games
    left join schools on team_games.school_id = schools.school_id
    left join sets on team_games.set_id = sets.set_id
    where team_games.school_id is not null
    and school_name is not null
    and sets.difficulty <> 'easy'
    GROUP BY 1
    ORDER BY Wins desc
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for tournament in res:
        if tournament["slug"]:
            tournament[
                "School"
            ] = f"<a href = '../schools/{tournament['slug']}'>{tournament['School']}</a>"

    records.append(res)

    # School Highest Winning Percentage
    cur.execute(
        f"""
            SELECT Team, Tournaments, printf("%.3f", \"Win%\") as \"Win%\"
            FROM (SELECT team as \"Team\", 
    count(distinct tournament_id) as Tournaments,
    avg(result) as \"Win%\"
    from team_games
    left join schools on team_games.school_id = schools.school_id
    left join teams on team_games.team_id = teams.team_id
    left join sets on team_games.set_id = sets.set_id
    where schools.open is null and schools.high_school is null
    and team_games.school_id is not null
    and sets.difficulty <> 'easy'
    GROUP BY 1)
    WHERE Tournaments >= 10
    ORDER BY 3 desc
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    records.append([dict(zip(keys, row)) for row in rows])

    # Most Tournaments Won
    cur.execute(
        f"""
            SELECT school_name as \"School\", slug,
    count(distinct tournament_results.tournament_id) as Tournaments,
    sum(case rank when 1 then 1 else 0 end) as Wins
    from tournament_results
    left join teams on tournament_results.team_id = teams.team_id
    left join schools on teams.school_id = schools.school_id
    left join tournaments on tournament_results.tournament_id = tournaments.tournament_id
    left join sets on tournaments.set_id = sets.set_id
    where teams.school_id is not null
    and sets.difficulty <> 'easy'
    GROUP BY 1
    ORDER BY 3 desc
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for tournament in res:
        if tournament["slug"]:
            tournament[
                "School"
            ] = f"<a href = '../schools/{tournament['slug']}'>{tournament['School']}</a>"

    records.append(res)

    # Nats titles
    cur.execute(
        f"""SELECT champions.*, schools.slug from champions
    left join teams on champions.team_id = teams.team_id
    left join schools on teams.school_id = schools.school_id"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for tournament in res:
        if tournament["slug"]:
            tournament[
                "school"
            ] = f"<a href = '../schools/{tournament['slug']}'>{tournament['school']}</a>"
    df = (
        pd.DataFrame(res)
        .groupby(["tournament", "school"])["year"]
        .agg("count")
        .reset_index()
        .pivot(columns="tournament", values="year", index="school")
        .reset_index()
        .fillna(0)
        .assign(Total=lambda x: x["ACF Nationals"] + x["DI ICT"])
        .sort_values(["Total"], ascending=False)
    )

    records.append(df.to_dict("records"))

    # Most Team Wins in a Season
    cur.execute(
        f"""
    SELECT year as Season, team as \"Team\", 
    count(distinct tournament_id) as Tournaments,
    sum(case result when 1 then 1 else 0 end) as Wins
    from team_games
    left join teams on team_games.team_id = teams.team_id
    left join schools on teams.school_id = schools.school_id
    left join sets on team_games.set_id = sets.set_id
    where teams.school_id is not null
    and sets.difficulty <> 'easy'
    GROUP BY 1, 2
    ORDER BY 4 desc
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en["Season"] = f"<a href = '../seasons/{en['Season']}'>{en['Season']}</a>"

    records.append(res)

    # Most Team Wins in a Season
    cur.execute(
        f"""
    SELECT sets.year as Season, team as \"Team\", 
    count(distinct tournament_results.tournament_id) as Tournaments,
    sum(case rank when 1 then 1 else 0 end) as Wins
    from tournament_results
    left join teams on tournament_results.team_id = teams.team_id
    left join schools on teams.school_id = schools.school_id
    left join tournaments on tournament_results.tournament_id = tournaments.tournament_id
    left join sets on tournaments.set_id = sets.set_id
    where teams.school_id is not null
    and sets.difficulty <> 'easy'
    GROUP BY 1, 2
    ORDER BY 4 desc
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en["Season"] = f"<a href = '../seasons/{en['Season']}'>{en['Season']}</a>"

    records.append(res)

    # Highest National Tournament Team PP20TUH
    cur.execute(
        f"""
    SELECT Season, Tournament, \"set\", site, tournament_id, Team, printf("%.2f", PP20TUH) as PP20TUH
    FROM (
    SELECT sets.year as Season,
    \"set\" as Tournament, \"set\", site, team as \"Team\", team_games.tournament_id,
    round(sum(total_pts)*20/sum(ifnull(tuh, 20)), 2) as PP20TUH
    from team_games
    left join teams on team_games.team_id = teams.team_id
    left join tournaments on team_games.tournament_id = tournaments.tournament_id
    left join sets on tournaments.set_id = sets.set_id
    left join sites on tournaments.site_id = sites.site_id
    where teams.school is not null
    and \"set\" in ('DI ICT', 'ACF Nationals')
    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY 7 desc
    LIMIT 10)"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Team"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

    records.append(res)

    # Highest National Tournament Team TU%
    cur.execute(
        f"""
    SELECT Season, Tournament, \"set\", site, Team, tournament_id, printf("%.3f", \"TU%\") as \"TU%\"
    FROM (
    SELECT sets.year as Season,
    \"set\" as Tournament, \"set\", site, team as \"Team\", team_games.tournament_id,
    (sum(ifnull(powers, 0))+sum(tens))/sum(ifnull(tuh, 20)) as \"TU%\"
    from team_games
    left join teams on team_games.team_id = teams.team_id
    left join tournaments on team_games.tournament_id = tournaments.tournament_id
    left join sets on tournaments.set_id = sets.set_id
    left join sites on tournaments.site_id = sites.site_id
    where teams.school is not null
    and \"set\" in ('DI ICT', 'ACF Nationals')
    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY 7 desc
    LIMIT 10)"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Team"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

    records.append(res)

    # Highest National Tournament PPB
    cur.execute(
        f"""
    SELECT sets.year as Season, team_games.tournament_id,
    \"set\" as Tournament, \"set\", site, team as \"Team\", 
    round((sum(bonus_pts)/sum(bonuses_heard)), 2) as PPB
    from team_games
    left join teams on team_games.team_id = teams.team_id
    left join tournaments on team_games.tournament_id = tournaments.tournament_id
    left join sets on tournaments.set_id = sets.set_id
    left join sites on tournaments.site_id = sites.site_id
    where teams.school is not null
    and sets.\"set\" in ('DI ICT', 'ACF Nationals')
    GROUP BY 1, 2, 3, 4, 5, 6"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    df = pd.DataFrame([dict(zip(keys, row)) for row in rows])
    df_mean = (
        df.groupby(["Season", "Tournament"])["PPB"]
        .agg("mean")
        .reset_index()
        .rename(columns={"PPB": "mean"})
    )
    df_sd = (
        df.groupby(["Season", "Tournament"])["PPB"]
        .agg("std")
        .reset_index()
        .rename(columns={"PPB": "sd"})
    )
    df = df.merge(df_mean, on=["Season", "Tournament"])
    df = df.merge(df_sd, on=["Season", "Tournament"])
    df["z"] = round(((df["PPB"] - df["mean"]) / df["sd"]), 2)
    df = df.sort_values("z", ascending=False).nlargest(10, "z")
    df[["mean", "sd", "z"]] = df[["mean", "sd", "z"]].apply(lambda x: round(x, 2))
    res = df.to_dict("records")
    for en in res:
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Team"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

    records.append(res)

    # Highest Tournament Team PP20TUH
    cur.execute(
        f"""
    SELECT Season, Tournament, \"set\", site, Team, tournament_id, printf("%.2f", PP20TUH) as PP20TUH
    FROM (
    SELECT sets.year as Season,
    \"set\" as Tournament, \"set\", site, team as \"Team\", team_games.tournament_id,
    round(sum(total_pts)*20/sum(ifnull(tuh, 20)), 2) as PP20TUH
    from team_games
    left join teams on team_games.team_id = teams.team_id
    left join tournaments on team_games.tournament_id = tournaments.tournament_id
    left join sets on tournaments.set_id = sets.set_id
    left join sites on tournaments.site_id = sites.site_id
    where teams.school is not null
    and sets.difficulty <> 'easy'
    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY 7 desc
    LIMIT 10)"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Team"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

    records.append(res)

    # Highest Tournament Team TU%
    cur.execute(
        f"""
    SELECT Season, Tournament, \"set\", site, Team, tournament_id, printf("%.3f", \"TU%\") as \"TU%\"
    FROM (
    SELECT sets.year as Season,
    \"set\" as Tournament, \"set\", site, team as \"Team\", team_games.tournament_id,
    (sum(ifnull(powers, 0))+sum(tens))/sum(ifnull(tuh, 20)) as \"TU%\"
    from team_games
    left join teams on team_games.team_id = teams.team_id
    left join tournaments on team_games.tournament_id = tournaments.tournament_id
    left join sets on tournaments.set_id = sets.set_id
    left join sites on tournaments.site_id = sites.site_id
    where teams.school is not null
    and sets.difficulty <> 'easy'
    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY 7 desc
    LIMIT 10)"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Team"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

    records.append(res)

    # Highest Tournament PPB
    cur.execute(
        f"""
    SELECT sets.year as Season, team_games.tournament_id,
    \"set\" as Tournament, \"set\", site, team as \"Team\", 
    round((sum(bonus_pts)/sum(bonuses_heard)), 2) as PPB
    from team_games
    left join teams on team_games.team_id = teams.team_id
    left join tournaments on team_games.tournament_id = tournaments.tournament_id
    left join sets on tournaments.set_id = sets.set_id
    left join sites on tournaments.site_id = sites.site_id
    where teams.school is not null
    and sets.difficulty <> 'easy'
    GROUP BY 1, 2, 3, 4, 5, 6"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    df = pd.DataFrame([dict(zip(keys, row)) for row in rows])
    df_mean = (
        df.groupby(["Season", "Tournament"])["PPB"]
        .agg("mean")
        .reset_index()
        .rename(columns={"PPB": "mean"})
    )
    df_sd = (
        df.groupby(["Season", "Tournament"])["PPB"]
        .agg("std")
        .reset_index()
        .rename(columns={"PPB": "sd"})
    )
    df = df.merge(df_mean, on=["Season", "Tournament"])
    df = df.merge(df_sd, on=["Season", "Tournament"])
    df["z"] = round(((df["PPB"] - df["mean"]) / df["sd"]), 2)
    df = df.sort_values("z", ascending=False).nlargest(10, "z")
    df[["mean", "sd", "z"]] = df[["mean", "sd", "z"]].apply(lambda x: round(x, 2))
    res = df.to_dict("records")
    for en in res:
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Team"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

    records.append(res)

    # Most Points in a Game, Winning Team
    cur.execute(
        f"""
    SELECT sets.year as Season,
    \"set\" as Tournament, \"set\", site, team as \"Team\", team_games.tournament_id,
    game_id, ifnull(tuh, 20) as TUH,
    total_pts as Pts
    from team_games
    left join teams on team_games.team_id = teams.team_id
    left join tournaments on team_games.tournament_id = tournaments.tournament_id
    left join sets on tournaments.set_id = sets.set_id
    left join sites on tournaments.site_id = sites.site_id
    where teams.school is not null
    and sets.difficulty <> 'easy'
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    ORDER BY total_pts desc
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Team"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"
        en[
            "Pts"
        ] = f"<a href = '../games/{utils.string(en['game_id'])}'>{round(en['Pts'])}</a>"

    records.append(res)

    # Most PP20TUH in a Full Game, Winning Team
    cur.execute(
        f"""
    SELECT sets.year as Season,
    \"set\" as Tournament, \"set\", site, team as \"Team\", team_games.tournament_id,
    game_id, ifnull(tuh, 20) as TUH,
    total_pts as Pts,
    round(total_pts*20/ifnull(tuh, 20), 2) as PP20TUH
    from team_games
    left join teams on team_games.team_id = teams.team_id
    left join tournaments on team_games.tournament_id = tournaments.tournament_id
    left join sets on tournaments.set_id = sets.set_id
    left join sites on tournaments.site_id = sites.site_id
    where teams.school is not null
    and sets.difficulty <> 'easy'
    and ifnull(tuh, 20) > 12
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    ORDER BY PP20TUH desc
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Team"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"
        en[
            "PP20TUH"
        ] = f"<a href = '../games/{utils.string(en['game_id'])}'>{en['PP20TUH']}</a>"

    records.append(res)

    # Most PP20TUH in a Full Game, Winning Team
    cur.execute(
        f"""
    SELECT sets.year as Season,
    \"set\" as Tournament, \"set\", site, team_games.tournament_id, 
    game_id, 
    group_concat(team, ' vs. ') as Teams, 
    group_concat(cast(total_pts as string), ' - ') as Score, 
    ifnull(avg(tuh), 20) as TUH,
    sum(total_pts) as Pts
    from team_games
    left join teams on team_games.team_id = teams.team_id
    left join tournaments on team_games.tournament_id = tournaments.tournament_id
    left join sets on tournaments.set_id = sets.set_id
    left join sites on tournaments.site_id = sites.site_id
    where teams.school is not null
    and sets.difficulty <> 'easy'
    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY Pts desc
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en["Score"] = en["Score"].replace(".0", "")
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Pts"
        ] = f"<a href = '../games/{utils.string(en['game_id'])}'>{round(en['Pts'])}</a>"

    records.append(res)

    # Most PP20TUH in a Game, Both Teams
    cur.execute(
        f"""
    SELECT sets.year as Season,
    \"set\" as Tournament, \"set\", site,
    game_id, team_games.tournament_id,
    group_concat(team, ' vs. ') as Teams, 
    group_concat(cast(total_pts as string), ' - ') as Score, 
    sum(total_pts) as Pts,
    ifnull(avg(tuh), 20) as TUH,
    round(sum(total_pts)*20/ifnull(avg(tuh), 20), 2) as PP20TUH
    from team_games
    left join teams on team_games.team_id = teams.team_id
    left join tournaments on team_games.tournament_id = tournaments.tournament_id
    left join sets on tournaments.set_id = sets.set_id
    left join sites on tournaments.site_id = sites.site_id
    where teams.school is not null
    and sets.difficulty <> 'easy'
    and ifnull(tuh, 20) > 12
    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY PP20TUH desc
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en["Score"] = en["Score"].replace(".0", "")
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "PP20TUH"
        ] = f"<a href = '../games/{utils.string(en['game_id'])}'>{en['PP20TUH']}</a>"

    records.append(res)

    # Most Negs in a Game, Winning Team
    cur.execute(
        f"""
    SELECT sets.year as Season,
    \"set\" as Tournament, \"set\", site,
    game_id, 
    team as Team,team_games.tournament_id,
    case result when 1 then 'W' when 0 then 'L' else 'T' end as Result,
    ifnull(tuh, 20) as TUH,
    powers as \"15\", tens as \"10\", negs as \"-5\",
    total_pts as Pts
    from team_games
    left join teams on team_games.team_id = teams.team_id
    left join tournaments on team_games.tournament_id = tournaments.tournament_id
    left join sets on tournaments.set_id = sets.set_id
    left join sites on tournaments.site_id = sites.site_id
    where teams.school is not null
    and sets.difficulty <> 'easy'
    and ifnull(powers, 0) + tens + negs <= ifnull(tuh, 20)
    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY negs desc
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Team"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"
        en[
            "-5"
        ] = f"<a href = '../games/{utils.string(en['game_id'])}'>{round(en['-5'])}</a>"

    records.append(res)

    # Most Negs in a Game, Winning Team
    cur.execute(
        f"""
    SELECT sets.year as Season,
    \"set\" as Tournament, \"set\", site,
    game_id, 
    team as Team,team_games.tournament_id,
    case result when 1 then 'W' when 0 then 'L' else 'T' end as Result,
    ifnull(tuh, 20) as TUH,
    powers as \"15\", tens as \"10\", negs as \"-5\",
    bonuses_heard as BHrd,
    bonus_pts as BPts,
    round(bonus_pts/bonuses_heard, 2) as PPB
    from team_games
    left join teams on team_games.team_id = teams.team_id
    left join tournaments on team_games.tournament_id = tournaments.tournament_id
    left join sets on tournaments.set_id = sets.set_id
    left join sites on tournaments.site_id = sites.site_id
    where teams.school is not null
    and sets.difficulty <> 'easy'
    and bonuses_heard >= 8
    order by PPB desc
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Team"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"
        en[
            "PPB"
        ] = f"<a href = '../games/{utils.string(en['game_id'])}'>{en['PPB']}</a>"

    records.append(res)

    # Grails
    cur.execute(
        f"""
    SELECT sets.year as Season,
    \"set\" as Tournament, \"set\", site,
    game_id, team_games.tournament_id,
    team as Team, 
    opponent as Opponent,
    ifnull(tuh, 20) as TUH,
    powers as \"15\", tens as \"10\", negs as \"-5\",
    total_pts as Pts
    from team_games
    left join teams on team_games.team_id = teams.team_id
    left join tournaments on team_games.tournament_id = tournaments.tournament_id
    left join sets on tournaments.set_id = sets.set_id
    left join sites on tournaments.site_id = sites.site_id
    where teams.school is not null
    and sets.difficulty <> 'easy'
    and ifnull(powers, 0) + tens = ifnull(tuh, 20)
    and negs = 0
    and ifnull(tuh, 20) > 12
    ORDER BY Pts desc"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Team"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"
        en[
            "Pts"
        ] = f"<a href = '../games/{utils.string(en['game_id'])}'>{round(en['Pts'])}</a>"

    records.append(res)

    # Most Player Points Scored
    cur.execute(
        f"""
    SELECT Player, slug, Ts, GP, printf("%,d", Pts) as Pts
    from (
    SELECT fname || ' ' || lname as Player, slug,
    count(distinct tournament_id) as Ts,
    count(tournament_id) as GP,
    sum(pts) as Pts
    from player_games
    left join teams on player_games.team_id = teams.team_id
    left join sets on player_games.set_id = sets.set_id
    LEFT JOIN players on player_games.player_id = players.player_id
    LEFT JOIN people on players.person_id = people.person_id
    where teams.school_id is not null
    and sets.difficulty <> 'easy'
    and fname is not null
    GROUP BY 1, 2
    ORDER BY Pts desc
    LIMIT 10)"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        if en["slug"]:
            en["Player"] = f"<a href = '../players/{en['slug']}'>{en['Player']}</a>"

    records.append(res)

    # Most Player Tournaments Played
    cur.execute(
        f"""SELECT fname || ' ' || lname as Player, slug,
    replace(group_concat(distinct school), ',', ', ') as Schools,
    count(distinct tournament_id) as Ts
    from player_games
    left join teams on player_games.team_id = teams.team_id
    left join sets on player_games.set_id = sets.set_id
    LEFT JOIN players on player_games.player_id = players.player_id
    LEFT JOIN people on players.person_id = people.person_id
    where teams.school_id is not null
    and sets.difficulty <> 'easy'
    and fname is not null
    GROUP BY 1, 2
    ORDER BY Ts desc
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for entry in res:
        if entry["slug"]:
            entry[
                "Player"
            ] = f"<a href = '../players/{entry['slug']}'>{entry['Player']}</a>"

    records.append(res)

    # Most Player National Tournaments Played
    cur.execute(
        f"""SELECT fname || ' ' || lname as Player, slug,
            replace(group_concat(distinct school), ',', ', ') as Schools,
    count(distinct tournament_id) as Ts
    from player_games
    left join teams on player_games.team_id = teams.team_id
    left join sets on player_games.set_id = sets.set_id
    LEFT JOIN players on player_games.player_id = players.player_id
    LEFT JOIN people on players.person_id = people.person_id
    where teams.school_id is not null
    and sets.difficulty <> 'easy'
    and fname is not null
    and sets.\"set\" in ('DI ICT', 'ACF Nationals')
    GROUP BY 1, 2
    ORDER BY Ts desc
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for entry in res:
        if entry["slug"]:
            entry[
                "Player"
            ] = f"<a href = '../players/{entry['slug']}'>{entry['Player']}</a>"

    records.append(res)

    # Most Player Wins
    cur.execute(
        f"""SELECT fname || ' ' || lname as Player, slug,
    replace(group_concat(distinct school), ',', ', ') as Schools,
    sum(case when result = 1 then 1 else 0 end) as Wins
    from player_games
    left join (select game_id, team_id, result from team_games) results on player_games.team_id = results.team_id and player_games.game_id = results.game_id
    left join teams on player_games.team_id = teams.team_id
    left join sets on player_games.set_id = sets.set_id
    LEFT JOIN players on player_games.player_id = players.player_id
    LEFT JOIN people on players.person_id = people.person_id
    where teams.school_id is not null
    and sets.difficulty <> 'easy'
    and fname is not null
    GROUP BY 1, 2
    ORDER BY Wins desc
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for entry in res:
        if entry["slug"]:
            entry[
                "Player"
            ] = f"<a href = '../players/{entry['slug']}'>{entry['Player']}</a>"

    records.append(res)

    # Most Tournament Player Wins
    cur.execute(
        f"""
    SELECT fname || ' ' || lname as Player, slug,
    replace(group_concat(distinct school), ',', ', ') as Schools,
    sum(case when rank = 1 then 1 else 0 end) as Wins
    from (select distinct player_id, tournament_id, set_id, team_id from player_games) player_games
    left join tournament_results on player_games.team_id = tournament_results.team_id 
    and player_games.tournament_id = tournament_results.tournament_id
    left join teams on player_games.team_id = teams.team_id
    left join sets on player_games.set_id = sets.set_id
    LEFT JOIN players on player_games.player_id = players.player_id
    LEFT JOIN people on players.person_id = people.person_id
    where teams.school_id is not null
    and sets.difficulty <> 'easy'
    and fname is not null
    GROUP BY 1, 2
    ORDER BY Wins desc
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for entry in res:
        if entry["slug"]:
            entry[
                "Player"
            ] = f"<a href = '../players/{entry['slug']}'>{entry['Player']}</a>"

    records.append(res)

    # Highest Player winning pct
    cur.execute(
        f"""
    select * from (
    SELECT fname || ' ' || lname as Player, slug,
    replace(group_concat(distinct school), ',', ', ') as Schools,
    count(player_games.game_id) as GP,
    printf("%.3f", avg(result)) as \"Win%\"
    from player_games
    left join (select game_id, team_id, result from team_games) results on player_games.team_id = results.team_id and player_games.game_id = results.game_id
    left join teams on player_games.team_id = teams.team_id
    left join sets on player_games.set_id = sets.set_id
    LEFT JOIN players on player_games.player_id = players.player_id
    LEFT JOIN people on players.person_id = people.person_id
    where teams.school_id is not null
    and sets.difficulty <> 'easy'
    and fname is not null
    GROUP BY 1, 2
    ORDER BY 5 desc)
    where GP >= 50
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for entry in res:
        if entry["slug"]:
            entry[
                "Player"
            ] = f"<a href = '../players/{entry['slug']}'>{entry['Player']}</a>"

    records.append(res)

    # Most Points in a Season
    cur.execute(
        f"""
    SELECT sets.year as Season, 
            school as School,
            fname || ' ' || lname as Player, people.slug as player_slug, schools.slug as school_slug,
    count(distinct tournament_id) as Ts,
    count(tournament_id) as GP,
    sum(pts) as Pts
    from player_games
    left join teams on player_games.team_id = teams.team_id
    left join schools on teams.school_id = schools.school_id
    left join sets on player_games.set_id = sets.set_id
    LEFT JOIN players on player_games.player_id = players.player_id
    LEFT JOIN people on players.person_id = people.person_id
    where teams.school_id is not null
    and sets.difficulty <> 'easy'
    and fname is not null
    GROUP BY 1, 2, 3
    ORDER BY Pts desc
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for entry in res:
        if entry["player_slug"]:
            entry[
                "Player"
            ] = f"<a href = '../players/{entry['player_slug']}'>{entry['Player']}</a>"
        if entry["school_slug"]:
            entry[
                "School"
            ] = f"<a href = '../schools/{entry['school_slug']}'>{entry['School']}</a>"

    records.append(res)

    # Highest PP20TUH in a Season
    cur.execute(
        f""" Select * from (
    SELECT sets.year as Season, 
            school as School,
            fname || ' ' || lname as Player, people.slug as player_slug, schools.slug as school_slug,
    count(distinct tournament_id) as Ts,
    count(tournament_id) as GP,
    sum(pts) as Pts,
    round(sum(pts)*20/sum(ifnull(tuh, 20)), 2) as PP20TUH
    from player_games
    left join teams on player_games.team_id = teams.team_id
    left join schools on teams.school_id = schools.school_id
    left join sets on player_games.set_id = sets.set_id
    LEFT JOIN players on player_games.player_id = players.player_id
    LEFT JOIN people on players.person_id = people.person_id
    where teams.school_id is not null
    and sets.difficulty <> 'easy'
    and fname is not null
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY PP20TUH desc)
    where Ts >= 5
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for entry in res:
        if entry["player_slug"]:
            entry[
                "Player"
            ] = f"<a href = '../players/{entry['player_slug']}'>{entry['Player']}</a>"
        if entry["school_slug"]:
            entry[
                "School"
            ] = f"<a href = '../schools/{entry['school_slug']}'>{entry['School']}</a>"

    records.append(res)

    # Highest PP20TUH in a Season
    cur.execute(
        f""" Select * from (
            SELECT sets.year as Season,
            fname || ' ' || lname as Player, people.slug as player_slug, schools.slug as school_slug,
    school as School,
    count(distinct tournament_id) as Ts,
    count(player_games.game_id) as GP,
    round(avg(result), 3) as \"Win%\"
    from player_games
    left join (select game_id, team_id, result from team_games) results on player_games.team_id = results.team_id and player_games.game_id = results.game_id
    left join teams on player_games.team_id = teams.team_id
    left join schools on teams.school_id = schools.school_id
    left join sets on player_games.set_id = sets.set_id
    LEFT JOIN players on player_games.player_id = players.player_id
    LEFT JOIN people on players.person_id = people.person_id
    where teams.school_id is not null
    and sets.difficulty <> 'easy'
    and fname is not null
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY \"Win%\" desc)
    where Ts >= 5
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for entry in res:
        if entry["player_slug"]:
            entry[
                "Player"
            ] = f"<a href = '../players/{entry['player_slug']}'>{entry['Player']}</a>"
        if entry["school_slug"]:
            entry[
                "School"
            ] = f"<a href = '../schools/{entry['school_slug']}'>{entry['School']}</a>"

    records.append(res)

    # Highest PP20TUH in a Tournament
    cur.execute(
        f""" Select *, printf("%.2f", rawPP20TUH) as PP20TUH from (SELECT sets.year as Season, 
    \"set\" as Tournament, \"set\", site, player_games.tournament_id,
    team as Team, fname || ' ' || lname as Player, people.slug as player_slug, schools.slug as school_slug,
    count(game_id) as GP,
    sum(pts) as Pts,
    round(sum(pts)*20/sum(ifnull(tuh, 20)), 2) as rawPP20TUH
    from player_games
    left join teams on player_games.team_id = teams.team_id
    left join schools on teams.school_id = schools.school_id
    left join tournaments on player_games.tournament_id = tournaments.tournament_id
    left join sets on player_games.set_id = sets.set_id
    left join sites on player_games.site_id = sites.site_id
    LEFT JOIN players on player_games.player_id = players.player_id
    LEFT JOIN people on players.person_id = people.person_id
    where teams.school_id is not null
    and sets.difficulty <> 'easy'
    and fname is not null
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    ORDER BY rawPP20TUH desc)
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en[
            "PP20TUH"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Player'])}-{utils.slug(en['Team'])}'>{en['PP20TUH']}</a>"
        if en["player_slug"]:
            en[
                "Player"
            ] = f"<a href = '../players/{en['player_slug']}'>{en['Player']}</a>"
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Team"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

    records.append(res)

    # Highest PP20TUH in a National Tournament
    cur.execute(
        f""" Select *, printf("%.2f", rawPP20TUH) as PP20TUH from (SELECT sets.year as Season, 
    \"set\" as Tournament, \"set\", site, player_games.tournament_id,
    team as Team, fname || ' ' || lname as Player, people.slug as player_slug, schools.slug as school_slug,
    count(game_id) as GP,
    sum(pts) as Pts,
    round(sum(pts)*20/sum(ifnull(tuh, 20)), 2) as rawPP20TUH
    from player_games
    left join teams on player_games.team_id = teams.team_id
    left join schools on teams.school_id = schools.school_id
    left join tournaments on player_games.tournament_id = tournaments.tournament_id
    left join sets on player_games.set_id = sets.set_id
    left join sites on player_games.site_id = sites.site_id
    LEFT JOIN players on player_games.player_id = players.player_id
    LEFT JOIN people on players.person_id = people.person_id
    where teams.school_id is not null
    and sets.\"set\" in ('DI ICT', 'ACF Nationals')
    and fname is not null
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    ORDER BY rawPP20TUH desc)
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en[
            "PP20TUH"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Player'])}-{utils.slug(en['Team'])}'>{en['PP20TUH']}</a>"
        if en["player_slug"]:
            en[
                "Player"
            ] = f"<a href = '../players/{en['player_slug']}'>{en['Player']}</a>"
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Team"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

    records.append(res)

    # Most Negs per 20 tuh Tournament
    cur.execute(
        f""" Select * from (SELECT sets.year as Season, 
    \"set\" as Tournament, \"set\", site, player_games.tournament_id,
    team as Team, fname || ' ' || lname as Player, people.slug as player_slug, schools.slug as school_slug,
    count(game_id) as GP,
    sum(negs) as \"-5\",
    round(sum(negs)*20/sum(ifnull(tuh, 20)), 2) as \"-5P20TUH\"
    from player_games
    left join teams on player_games.team_id = teams.team_id
    left join schools on teams.school_id = schools.school_id
    left join tournaments on player_games.tournament_id = tournaments.tournament_id
    left join sets on player_games.set_id = sets.set_id
    left join sites on player_games.site_id = sites.site_id
    LEFT JOIN players on player_games.player_id = players.player_id
    LEFT JOIN people on players.person_id = people.person_id
    where teams.school_id is not null
    and sets.difficulty <> 'easy'
    and fname is not null
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    ORDER BY \"-5P20TUH\" desc)
    where GP >= 5
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en[
            "-5P20TUH"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Player'])}-{utils.slug(en['Team'])}'>{en['-5P20TUH']}</a>"
        if en["player_slug"]:
            en[
                "Player"
            ] = f"<a href = '../players/{en['player_slug']}'>{en['Player']}</a>"
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Team"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

    records.append(res)

    # Most Player points in a game
    cur.execute(
        f""" Select * from (SELECT sets.year as Season,
    \"set\" as Tournament, \"set\", site, 
            team as Team, fname || ' ' || lname as Player, people.slug as player_slug,
            game_id, player_games.tournament_id,
    ifnull(tuh, 20) as TUH,
    powers as \"15\", tens as \"10\", negs as \"-5\",
    pts as Pts
    from player_games
    left join teams on player_games.team_id = teams.team_id
    left join tournaments on player_games.tournament_id = tournaments.tournament_id
    left join sets on player_games.set_id = sets.set_id
    left join sites on player_games.site_id = sites.site_id
    LEFT JOIN players on player_games.player_id = players.player_id
    LEFT JOIN people on players.person_id = people.person_id
    where teams.school_id is not null
    and sets.difficulty <> 'easy'
    and fname is not null
    ORDER BY Pts desc)
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en[
            "Pts"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Player'])}-{utils.slug(en['Team'])}'>{round(en['Pts'])}</a>"
        if en["player_slug"]:
            en[
                "Player"
            ] = f"<a href = '../players/{en['player_slug']}'>{en['Player']}</a>"
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Team"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

    records.append(res)

    # Most Player points in a national tournament game
    cur.execute(
        f""" Select * from (SELECT sets.year as Season,
    \"set\" as Tournament, \"set\", site, 
            team as Team, fname || ' ' || lname as Player, people.slug as player_slug,
            game_id,player_games.tournament_id,
    ifnull(tuh, 20) as TUH,
    powers as \"15\", tens as \"10\", negs as \"-5\",
    pts as Pts
    from player_games
    left join teams on player_games.team_id = teams.team_id
    left join tournaments on player_games.tournament_id = tournaments.tournament_id
    left join sets on player_games.set_id = sets.set_id
    left join sites on player_games.site_id = sites.site_id
    LEFT JOIN players on player_games.player_id = players.player_id
    LEFT JOIN people on players.person_id = people.person_id
    where teams.school_id is not null
    and sets.difficulty <> 'easy'
    and fname is not null
    and sets.\"set\" in ('DI ICT', 'ACF Nationals')
    ORDER BY Pts desc)
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en[
            "Pts"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Player'])}-{utils.slug(en['Team'])}'>{round(en['Pts'])}</a>"
        if en["player_slug"]:
            en[
                "Player"
            ] = f"<a href = '../players/{en['player_slug']}'>{en['Player']}</a>"
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Team"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

    records.append(res)

    # Most Tossups game
    cur.execute(
        f""" Select * from (SELECT sets.year as Season,
    \"set\" as Tournament, \"set\", site, 
            team as Team, fname || ' ' || lname as Player, people.slug as player_slug,
            game_id,player_games.tournament_id,
    ifnull(tuh, 20) as TUH,
    powers as \"15\", tens as \"10\", negs as \"-5\",
    ifnull(powers, 0) + tens as Tossups
    from player_games
    left join teams on player_games.team_id = teams.team_id
    left join tournaments on player_games.tournament_id = tournaments.tournament_id
    left join sets on player_games.set_id = sets.set_id
    left join sites on player_games.site_id = sites.site_id
    LEFT JOIN players on player_games.player_id = players.player_id
    LEFT JOIN people on players.person_id = people.person_id
    where teams.school_id is not null
    and sets.difficulty <> 'easy'
    and fname is not null
    ORDER BY Tossups desc)
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en[
            "Tossups"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Player'])}-{utils.slug(en['Team'])}'>{round(en['Tossups'])}</a>"
        if en["player_slug"]:
            en[
                "Player"
            ] = f"<a href = '../players/{en['player_slug']}'>{en['Player']}</a>"
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Team"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

    records.append(res)

    # Most Negs game
    cur.execute(
        f""" Select * from (SELECT sets.year as Season,
    \"set\" as Tournament, \"set\", site, 
            team as Team, fname || ' ' || lname as Player, people.slug as player_slug,
            game_id,player_games.tournament_id,
    ifnull(tuh, 20) as TUH,
    powers as \"15\", tens as \"10\", negs as \"-5\"
    from player_games
    left join teams on player_games.team_id = teams.team_id
    left join tournaments on player_games.tournament_id = tournaments.tournament_id
    left join sets on player_games.set_id = sets.set_id
    left join sites on player_games.site_id = sites.site_id
    LEFT JOIN players on player_games.player_id = players.player_id
    LEFT JOIN people on players.person_id = people.person_id
    where teams.school_id is not null
    and sets.difficulty <> 'easy'
    and fname is not null
    ORDER BY negs desc)
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for en in res:
        en[
            "-5"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Player'])}-{utils.slug(en['Team'])}'>{round(en['-5'])}</a>"
        if en["player_slug"]:
            en[
                "Player"
            ] = f"<a href = '../players/{en['player_slug']}'>{en['Player']}</a>"
        en[
            "Tournament"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en[
            "Team"
        ] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

    records.append(res)

    # Most Tournaments Hosted
    cur.execute(
        f"""SELECT school as School, slug,
    count(distinct tournaments.tournament_id) as Tournaments
    from tournaments
    left join sites on tournaments.site_id = sites.site_id
    left join schools on sites.school = schools.school_name
    where school is not null
    GROUP BY 1, 2
    ORDER BY 3 desc
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for entry in res:
        if entry["slug"]:
            entry[
                "School"
            ] = f"<a href = '../schools/{entry['slug']}'>{entry['School']}</a>"

    records.append(res)

    # Most Tournaments Hosted
    cur.execute(
        f"""SELECT 
            tournaments.tournament_id,
            sets.year as Year, \"set\" as \"Tournament\",
            site as Host, school, slug,
    count(distinct team_id) as Teams
    from team_games
    left join sets on team_games.set_id = sets.set_id
    left join tournaments on team_games.tournament_id = tournaments.tournament_id
    left join sites on tournaments.site_id = sites.site_id
    left join schools on sites.school = schools.school_name
    where school is not null
    and \"set\" not in ('DI ICT', 'ACF Nationals')
    GROUP BY 1
    ORDER BY Teams desc
    LIMIT 10"""
    )

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    res = [dict(zip(keys, row)) for row in rows]
    for entry in res:
        entry[
            "Teams"
        ] = f"<a href = '../tournaments/{utils.string(entry['tournament_id'])}'>{entry['Teams']}</a>"
        if entry["slug"]:
            entry[
                "Host"
            ] = f"<a href = '../schools/{entry['slug']}'>{entry['Host']}</a>"

    records.append(res)

    # print(records)
    # db.collection("records").document("current").set(records)

    with open("record-book.pkl", "wb") as handle:
        pickle.dump(records, handle, protocol=pickle.HIGHEST_PROTOCOL)


# with open('record-book.pkl', 'rb') as f:
#     records = pickle.load(f)
# record_book = {}
# for i, record in enumerate(records):
#     print(i, record)
#     db.collection("records").document(f'record{str(i)}').set({'records':record})

## Seasons
# cur.execute(
#     f"""
#         SELECT distinct sets.year from sets
#          """
# )
# season_slugs = ['22-23', '21-22', '20-21', '19-20', '18-19', '17-18', '16-17', '15-16', '14-15', '13-14', '12-13', '11-12']

# for season in season_slugs:
#     cur.execute(
#         f"""
#         SELECT
# sets.year as Season, tournament as Tournament, champions.tournament_id, \"set\", site, team, players
# from champions
# LEFT JOIN tournaments on champions.tournament_id = tournaments.tournament_id
# LEFT JOIN sets on tournaments.set_id = sets.set_id
# LEFT JOIN sites on tournaments.site_id = sites.site_id
# LEFT JOIN (SELECT player_games.tournament_id, team_id,
#            group_concat(distinct ' ' || coalesce(fname|| ' ' || lname, player_games.player)) as players
#             from player_games
#             LEFT JOIN schools on player_games.school_id = schools.school_id
#             LEFT JOIN players on player_games.player_id = players.player_id
#             LEFT JOIN people on players.person_id = people.person_id
#             LEFT JOIN tournaments on player_games.tournament_id = tournaments.tournament_id
#             LEFT JOIN sets on tournaments.set_id = sets.set_id
#             WHERE year = '{season}'
#             GROUP BY 1, 2) player_games
#            on champions.tournament_id = player_games.tournament_id
#            and champions.team_id = player_games.team_id
# where champions.year = '{season}'
#          """
#     )
#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     champs_res = [dict(zip(keys, row)) for row in rows]
#     for tournament in champs_res:
#         tournament[
#             "Tournament"
#         ] = f"<a href = '../tournaments/{str(tournament['tournament_id']).replace('.0', '')}'>{tournament['Tournament']}</a>"
#         tournament[
#             "team"
#         ] = f"<a href = '../tournaments/{str(tournament['tournament_id']).replace('.0', '')}#{utils.slug(tournament['team'])}'>{tournament['team']}</a>"

#     cur.execute(
#         f"""SELECT tournaments.*, results.Champion FROM (SELECT
#                 date as Date,
#                 sets.year, \"set\" as \"Set\", site as Site,
#                 \"set\" || ' at ' || site as Tournament, tournaments.tournament_id,
#                 count(distinct team_games.team_id) as Teams,
#                 count(distinct team_games.school_id) as Schools
#                 from team_games
#                 left join teams on team_games.team_id = teams.team_id
#                 left join tournaments on team_games.tournament_id = tournaments.tournament_id
#                 LEFT JOIN sets on tournaments.set_id = sets.set_id
#                 LEFT JOIN sites on tournaments.site_id = sites.site_id
#                 WHERE sets.year = '{season}'
#                 GROUP BY 1, 2, 3, 4, 5, 6
#                 ORDER BY Date) tournaments
#                 LEFT JOIN
#                 (
#                     SELECT
#                     date as Date,
#                     \"set\" || ' at ' || site as Tournament,
#                     teams.team as Champion
#                     from team_games
#                     left join teams on team_games.team_id = teams.team_id
#                     left join tournaments on team_games.tournament_id = tournaments.tournament_id
#                     LEFT JOIN sets on tournaments.set_id = sets.set_id
#                     LEFT JOIN sites on tournaments.site_id = sites.site_id
#                     LEFT JOIN tournament_results on team_games.tournament_id = tournament_results.tournament_id
#                     and team_games.team_id = tournament_results.team_id
#                     WHERE sets.year = '{season}'
#                     and rank = 1
#                     GROUP BY 1, 2
#                     ORDER BY Date desc
#                 ) results
#                 on tournaments.Tournament = results.Tournament
# """
#     )
#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     tournament_res = [dict(zip(keys, row)) for row in rows]
#     for tournament in tournament_res:
#         tournament[
#             "Tournament"
#         ] = f"<a href = '../tournaments/{str(tournament['tournament_id']).replace('.0', '')}'>{tournament['Tournament']}</a>"

#     cur.execute(
#         f"""SELECT
# sets.\"set\" as \"Set\", set_slug, difficulty,
#                 count(distinct team_games.site_id) as Sites,
#                 count(distinct team_games.team_id) as Teams,
#                 count(distinct team_games.school_id) as Schools
# from team_games
# LEFT JOIN sets on team_games.set_id = sets.set_id
# LEFT JOIN sites on team_games.site_id = sites.site_id
# left join editors on sets.set_id = editors.set_id
# LEFT JOIN schools on team_games.school_id = schools.school_id
# LEFT JOIN teams on team_games.team_id = teams.team_id
# WHERE sets.year = '{season}'
# GROUP BY 1,2,3
# order by difficulty, Teams
# """
#     )
#     diff_level = {"easy": 1, "medium": 2, "regionals": 3, "nationals": 4}

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     sets_res = [dict(zip(keys, row)) for row in rows]
#     sets_res = pd.DataFrame(sets_res)
#     sets_res["difficulty_lev"] = [diff_level[diff] for diff in sets_res["difficulty"]]
#     sets_res = (
#         pd.DataFrame(sets_res).sort_values(["difficulty_lev", "Set"]).to_dict("records")
#     )

#     for set in sets_res:
#         print(set['Set'])
#         set["Set"] = f"<a href = '../sets/{set['set_slug']}'>{set['Set']}</a>"
#         if set["difficulty"] == "easy":
#             set["difficulty"] = "<div class = 'diffdots easy-diff'>&#x25CF;</div>"
#         elif set["difficulty"] == "medium":
#             set[
#                 "difficulty"
#             ] = "<div class = 'diffdots medium-diff'>&#x25CF;&#x25CF;</div>"
#         elif set["difficulty"] == "regionals":
#             set[
#                 "difficulty"
#             ] = "<div class = 'diffdots regionals-diff'>&#x25CF;&#x25CF;&#x25CF;</div>"
#         else:
#             set[
#                 "difficulty"
#             ] = "<div class = 'diffdots nationals-diff'>&#x25CF;&#x25CF;&#x25CF;&#x25CF;</div>"

#     res = {"Champions": champs_res, "Tournaments": tournament_res, "Sets": sets_res}
#     db.collection("seasons").document(season).set(res)
#     print(season)

## Sets
# cur.execute(f"""SELECT sets.set_slug from sets """)
# set_slugs = [string(r[0]) for r in cur.fetchall() if r[0] is not None]

# for set_slug in set_slugs:
#     print(set_slug)
#     cur.execute(f"""
#         SELECT
# sets.year as Year,
# sets.\"set\" as \"Set\", difficulty, category, set_name, headitor
# from sets
# left join editors on sets.set_id = editors.set_id
#  WHERE sets.set_slug = '{set_slug}'
#  """)
#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     summary_res = [dict(zip(keys, row)) for row in rows]

#     cur.execute(f"""
#     SELECT
# sets.year as Year,
# sets.\"set\" as \"Set\", difficulty, set_name, headitor,
# category, subcategory as Subcategory, editor as Editor, slug
# from sets
# LEFT JOIN editors on sets.set_id = editors.set_id
# LEFT JOIN people on editors.person_id = people.person_id
# WHERE sets.set_slug = '{set_slug}'
# and category != 'Head'
# """)
#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     editor_res = [dict(zip(keys, row)) for row in rows]
#     for editor in editor_res:
#         if editor['slug']:
#             editor['Editor'] = f"<a href = '../players/{editor['slug']}'>{editor['Editor']}</a>"

#     if len(editor_res) > 0:
#         editor_res = pd.DataFrame(editor_res).groupby(
#             ['category', 'Subcategory']
#         )[['Editor']].agg(
#             lambda x: ', '.join(x)
#         ).reset_index().to_dict('records')

#     cur.execute(f"""SELECT
# sets.year as Year, tournaments.date as Date, team_games.tournament_id,
# \"set\" as \"Set\", site as Site, count(distinct team_id) as Teams,
# count(distinct(team_games.school_id)) as Schools,
# round(sum(powers)/sum(ifnull(tuh, 20)), 3) as \"15%\",
# printf("%.3f",(sum(ifnull(powers,0))+sum(tens))/sum(ifnull(tuh, 20)/2)) as \"Conv%\",
# printf("%.2f",sum(bonus_pts)/sum(bonuses_heard)) as PPB
# from team_games
# LEFT JOIN tournaments on team_games.tournament_id = tournaments.tournament_id
# LEFT JOIN sets on tournaments.set_id = sets.set_id
# LEFT JOIN sites on tournaments.site_id = sites.site_id
# WHERE sets.set_slug = '{set_slug}'
# GROUP BY 1,2,3,4
# """)
#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     tournament_res = [dict(zip(keys, row)) for row in rows]
#     for en in tournament_res:
#             en['Site'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Site']}</a>"

#     cur.execute(f"""SELECT
# sets.year as Year,
# ifnull(teams.team, school_name) as Team, team_games.tournament_id, schools.slug as school_slug,
# \"set\" as \"Set\", site as Site,
# count(game_id) as GP,
# sum(case result when 1 then 1 else 0 end) || '-' ||
# sum(case result when 0 then 1 else 0 end) as \"W-L\",
# sum(ifnull(tuh, 20)) as TUH,
# sum(powers) as \"15\", sum(tens) as \"10\", sum(negs) as \"-5\",
# printf("%.1f", sum(powers)/count(result)) as \"15/G\",
# printf("%.1f", sum(tens)/count(result)) as \"10/G\",
# printf("%.1f", sum(negs)/count(result)) as \"-5/G\",
# printf("%.3f",(sum(ifnull(powers, 0)) + sum(tens))/sum(ifnull(tuh, 20)), 3) as \"TU%\",
# printf("%.2f", avg(total_pts)) as PPG,
# printf("%.2f", sum(bonus_pts)/sum(bonuses_heard)) as PPB,
# printf("%.2f",a_value) as \"A-Value\"
# from team_games
# LEFT JOIN sets on team_games.set_id = sets.set_id
# LEFT JOIN sites on team_games.site_id = sites.site_id
# LEFT JOIN schools on team_games.school_id = schools.school_id
# LEFT JOIN teams on team_games.team_id = teams.team_id
# LEFT JOIN tournament_results on team_games.tournament_id = tournament_results.tournament_id
#         and team_games.team_id = tournament_results.team_id
# WHERE sets.set_slug = '{set_slug}'
# GROUP BY 1,2,3,4,5
# """)
#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     teams_res = [dict(zip(keys, row)) for row in rows]
#     for team in teams_res:
#         team['Site'] = f"<a href = '../tournaments/{str(team['tournament_id']).replace('.0', '')}'>{team['Site']}</a>"
#         team['Team'] = f"<a href = '../tournaments/{str(team['tournament_id']).replace('.0', '')}#{utils.slug(team['Team'])}'>{team['Team']}</a>"

#     cur.execute(f"""
#         SELECT
#         sets.year as Year, \"set\" as \"Set\", site as Site, player_games.tournament_id,
#         coalesce(fname|| ' ' || lname, player_games.player) as Player,
#         team as Team, slug,
#         count(tens) as GP,
#         sum(ifnull(tuh, 20)) as TUH,
#         sum(powers) as \"15\",
#         sum(tens) as \"10\",
#         sum(negs) as \"-5\",
#         printf("%.1f", sum(powers)/count(tens)) as \"15/G\",
#         printf("%.1f", sum(tens)/count(tens)) as \"10/G\",
#         printf("%.1f", sum(negs)/count(tens)) as \"-5/G\",
#         printf("%.2f", sum(powers)/sum(negs)) as \"P/N\",
#         printf("%.2f", (sum(ifnull(powers, 0)) + sum(tens))/sum(negs)) as \"G/N\",
#         printf("%.2f", avg(pts)) as PPG from
#         player_games
#         LEFT JOIN teams on player_games.team_id = teams.team_id
#         LEFT JOIN tournaments on player_games.tournament_id = tournaments.tournament_id
#         LEFT JOIN sets on player_games.set_id = sets.set_id
#         LEFT JOIN sites on player_games.site_id = sites.site_id
#         LEFT JOIN players on player_games.player_id = players.player_id
#         LEFT JOIN people on players.person_id = people.person_id
# WHERE sets.set_slug = '{set_slug}'
# GROUP BY 1,2,3,4,5, 6
# ORDER BY Player
# """)
#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     players_res = [dict(zip(keys, row)) for row in rows]
#     for player in players_res:
#         if player['slug']:
#             player['Player'] = f"<a href = '../players/{player['slug']}'>{player['Player']}</a>"
#         player['Team'] = f"<a href = '../tournaments/{str(player['tournament_id']).replace('.0', '')}#{utils.slug(player['Team'])}'>{player['Team']}</a>"


#     res = {
#         'Summary': summary_res,
#         'Editors': editor_res,
#         'Tournaments': tournament_res,
#         'Teams': teams_res,
#         'Players': players_res
#     }

#     db.collection("set_stats").document(set_slug).set(res)

# Schools
cur.execute("""SELECT distinct(slug) from
        team_games
        left join teams on team_games.team_id = teams.team_id
        left join schools on teams.school_id = schools.school_id
                    where slug is not null
                    and school_name is not null""")
team_slugs = [string(r[0]).replace(',', '') for r in cur.fetchall() if r[0] is not None]
team_slugs = ['city-university-of-london', 'queen-mary-university-of-london']
for team_slug in team_slugs:
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
            tournament['Team'] = f"<a href = '../tournaments/{str(tournament['tournament_id']).replace('.0', '')}/team-detail#{utils.slug(tournament['Team'])}'>{tournament['Team']}</a>"
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

    db.collection("schools").document(team_slug).update(res)
    print(team_slug)

# ## Tournaments
# cur.execute("SELECT tournament_id from tournaments")
# tournament_slugs = [string(r[0]) for r in cur.fetchall() if r[0] is not None]

# for tournament_id in tournament_slugs:
#     cur.execute(f"""
#         SELECT
#            date, tournaments.tournament_name, naqt_id
#            from tournaments
#            LEFT JOIN sets on tournaments.set_id = sets.set_id
#            LEFT JOIN sites on tournaments.site_id = sites.site_id
#            WHERE tournaments.tournament_id = {tournament_id}
#         """)

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     summary_res = [dict(zip(keys, row)) for row in rows]

#     cur.execute(f"""SELECT
#         rank as Rank,
#         teams.team as Team,
#         schools.school_name as School, slug, bracket,
#         count(result) as GP,
#         sum(case result when 1 then 1 else 0 end) || '-' ||
#         sum(case result when 0 then 1 else 0 end) as \"W-L\",
#         sum(ifnull(tuh, 20)) as TUH,
#         sum(powers) as \"15\", sum(tens) as \"10\", sum(negs) as \"-5\",
#         printf("%.2f", sum(powers)/count(result)) as \"15/G\",
#         printf("%.2f", sum(tens)/count(result)) as \"10/G\",
#         printf("%.2f", sum(negs)/count(result)) as \"-5/G\",
#         printf("%.3f", (sum(ifnull(powers, 0)) + sum(tens))/sum(ifnull(tuh, 20))) as \"TU%\",
#         printf("%.1f", avg(total_pts)) as PPG,
#         printf("%.2f", sum(bonus_pts)/sum(bonuses_heard)) as PPB,
#         printf("%.1f", a_value) as \"A-Value\"
#         from team_games
#         LEFT JOIN teams on team_games.team_id = teams.team_id
#         LEFT JOIN schools on teams.school_id = schools.school_id
#         LEFT JOIN tournaments on team_games.tournament_id = tournaments.tournament_id
#         LEFT JOIN tournament_results on team_games.tournament_id = tournament_results.tournament_id
#         and team_games.team_id = tournament_results.team_id
#         LEFT JOIN sets on tournaments.set_id = sets.set_id
#         LEFT JOIN sites on tournaments.site_id = sites.site_id
#         WHERE team_games.tournament_id = {tournament_id}
#         GROUP BY 1, 2, 3, 4, 5
#     """)

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     standings_res = [dict(zip(keys, row)) for row in rows]
#     for team in standings_res:
#         if team['slug']:
#             team['School'] = f"<a href = '../schools/{team['slug']}'>{team['School']}</a>"
#         team['Team'] = f"<a href = '/tournaments/{tournament_id}/team-detail#{utils.slug(team['Team'])}'>{team['Team']}</a>"

#     cur.execute(f"""
#     SELECT *, printf("%.2f", rawPPG) as PPG from (
#     SELECT
#         coalesce(fname|| ' ' || lname, player_games.player) as Player,
#         coalesce(fname|| ' ' || lname, player_games.player) as raw_player,
#         slug,
#         team as Team,
#         count(tens) as GP,
#         sum(ifnull(tuh, 20)) as TUH,
#         sum(powers) as \"15\",
#         sum(tens) as \"10\",
#         sum(negs) as \"-5\",
#         printf("%.2f", sum(powers)/count(tens)) as \"15/G\",
#         printf("%.2f", sum(tens)/count(tens)) as \"10/G\",
#         printf("%.2f", sum(negs)/count(tens)) as \"-5/G\",
#         printf("%.2f", sum(powers)/sum(negs)) as \"P/N\",
#         printf("%.2f", (sum(ifnull(powers, 0)) + sum(tens))/sum(negs)) as \"G/N\",
#         printf("%.3f", (sum(ifnull(powers, 0)) + sum(tens))/sum(ifnull(tuh, 20))) as \"TU%\",
#         avg(pts) as rawPPG from
#         player_games
#         LEFT JOIN teams on player_games.team_id = teams.team_id
#         LEFT JOIN tournaments on player_games.tournament_id = tournaments.tournament_id
#         LEFT JOIN sets on tournaments.set_id = sets.set_id
#         LEFT JOIN sites on tournaments.site_id = sites.site_id
#         LEFT JOIN players on player_games.player_id = players.player_id
#         LEFT JOIN people on players.person_id = people.person_id
#         WHERE player_games.tournament_id = {tournament_id}
#         GROUP BY 1, 2, 3, 4
#         ORDER BY rawPPG desc)
#     """)

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     players_res = [dict(zip(keys, row)) for row in rows]
#     for team in players_res:
#         team['Player'] = f"<a href = '/tournaments/{tournament_id}/player-detail#{utils.slug(team['Player']) + '-' + utils.slug(team['Team'])}'>{team['Player']}</a>"
#         team['Team'] = f"<a href = '/tournaments/{tournament_id}/team-detail#{utils.slug(team['Team'])}'>{team['Team']}</a>"

#     cur.execute(f"""
#     SELECT
#         CAST(REPLACE(round, 'Round ', '') as int) as Round,
#         team as Team,
#         game_num, game_id,
#         opponent_team as Opponent,
#         case result when 1 then 'W' when 0 then 'L' else 'T' end as Result,
#         total_pts as PF, opp_pts as PA, powers as \"15\", tens as \"10\",
#         negs as \"-5\", ifnull(tuh, 20) as TUH,
#         printf("%.2f", total_pts/ifnull(tuh, 20)) as PPTUH,
#         bonuses_heard as BHrd, bonus_pts as BPts,
#         printf("%.2f", bonus_pts/bonuses_heard) as PPB
#         from team_games
#         LEFT JOIN teams on team_games.team_id = teams.team_id
#         LEFT JOIN (select team_id, team as opponent_team from teams) a on team_games.opponent_id = a.team_id
#         where team_games.tournament_id = {tournament_id}
#         order by Team, Round
#     """)

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     team_detail_team_res = [dict(zip(keys, row)) for row in rows]
#     for team in team_detail_team_res:
#         team['Result'] = f"<a href = '../../games/{utils.string(team['game_id'])}'>{team['Result']}</a>"
#         if team['Opponent']:
#             team['Opponent'] = f"<a href = '/tournaments/{tournament_id}/team-detail#{utils.slug(team['Opponent'])}'>{team['Opponent']}</a>"


#     cur.execute(f"""
#     SELECT
#         coalesce(fname|| ' ' || lname, player_games.player) as Player,
#         team as Team,
#         count(tens) as GP,
#         sum(ifnull(tuh, 20)) as TUH,
#         sum(powers) as \"15\", sum(tens) as \"10\", sum(negs) as \"-5\",
#         printf("%.2f", sum(powers)/count(tens)) as \"15/G\",
#         printf("%.2f", sum(tens)/count(tens)) as \"10/G\",
#         printf("%.2f", sum(negs)/count(tens)) as \"-5/G\",
#         printf("%.2f", sum(powers)/sum(negs)) as \"P/N\",
#         printf("%.2f", (sum(ifnull(powers, 0)) + sum(tens))/sum(negs)) as \"G/N\",
#         printf("%.3f", (sum(ifnull(powers, 0)) + sum(tens))/sum(ifnull(tuh, 20))) as \"TU%\",
#         sum(pts) as Pts,
#         printf("%.2f", avg(pts)) as PPG from
#         player_games
#         LEFT JOIN teams on player_games.team_id = teams.team_id
#         LEFT JOIN tournaments on player_games.tournament_id = tournaments.tournament_id
#         LEFT JOIN players on player_games.player_id = players.player_id
#         LEFT JOIN people on players.person_id = people.person_id
#         where player_games.tournament_id = {tournament_id}
#         GROUP BY 1, 2
#         order by Team, Player
#     """)

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     team_detail_player_res = [dict(zip(keys, row)) for row in rows]
#     for team in team_detail_player_res:
#         team['Player'] = f"<a href = '/tournaments/{tournament_id}/player-detail#{utils.slug(team['Player']) + '-' + utils.slug(team['Team'])}'>{team['Player']}</a>"

#     cur.execute(f"""SELECT
# coalesce(fname|| ' ' || lname, player_games.player) as player, team,
# CAST(REPLACE(games.round, 'Round ', '') as int) as Round,
# opponent_team as Opponent,
# player_games.game_num, player_games.game_id,
# case result when 1 then 'W' when 0 then 'L' else 'T' end as Result,
# ifnull(player_games.tuh, 20) as TUH,
# player_games.powers as \"15\", player_games.tens as \"10\", player_games.negs as \"-5\", pts as Pts
# from player_games
# LEFT JOIN team_games on player_games.game_id = team_games.game_id
# and player_games.team_id = team_games.team_id
# LEFT JOIN (select team_id, team as opponent_team from teams) a on team_games.opponent_id = a.team_id
#         LEFT JOIN players on player_games.player_id = players.player_id
#         LEFT JOIN people on players.person_id = people.person_id
#         LEFT JOIN teams on player_games.team_id = teams.team_id
#         left join games on player_games.game_id = games.game_id
# WHERE player_games.tournament_id = {tournament_id}
#         order by team, player, Round
#     """)

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     player_detail_res = [dict(zip(keys, row)) for row in rows]
#     for team in player_detail_res:
#         team['Result'] = f"<a href = '../../games/{utils.string(team['game_id'])}'>{team['Result']}</a>"
#         if team['Opponent']:
#             team['Opponent'] = f"<a href = '/tournaments/{tournament_id}/team-detail#{utils.slug(team['Opponent'])}'>{team['Opponent']}</a>"

#     res = {
#         'Summary': summary_res,
#         'Standings': standings_res,
#         'Players': players_res,
#         'Team Detail Teams': team_detail_team_res,
#         'Team Detail Players': team_detail_player_res,
#         'Player Detail': player_detail_res
#     }
#     for k, v in res.items():
#         db.collection("tournaments").document(tournament_id).collection('results').document(k).set({k:v})
#     print(tournament_id)

## Games
# cur.execute("select game_id from games")
# game_slugs = [string(r[0]) for r in cur.fetchall() if r[0] is not None]

# for game_id in game_slugs:
# cur.execute(
#     f"""
#     SELECT
#        games.round, teams.team, tournaments.tournament_name, tournaments.tournament_id
#        from team_games
#        LEFT JOIN games on team_games.game_id = games.game_id
#        LEFT JOIN tournaments on games.tournament_id = tournaments.tournament_id
#        LEFT JOIN teams on team_games.team_id = teams.team_id
#        LEFT JOIN sets on tournaments.set_id = sets.set_id
#        LEFT JOIN sites on tournaments.site_id = sites.site_id
#        WHERE team_games.game_id = {game_id}
#     """
# )

# rows = cur.fetchall()
# keys = [k[0] for k in cur.description]
# summary_res = [dict(zip(keys, row)) for row in rows]

#     cur.execute(
#         f"""SELECT
#     tournament_id,
# game_id,
# team,
# coalesce(fname|| ' ' || lname, player_games.player) as Player,
# ifnull(tuh, 20) as TUH,
# powers as \"15\", tens as \"10\", negs as \"-5\", pts as Pts
# from player_games
# LEFT JOIN teams on player_games.team_id = teams.team_id
# LEFT JOIN players on player_games.player_id = players.player_id
# LEFT JOIN people on players.person_id = people.person_id
# where game_id = {game_id}
#     """
#     )

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     players_res = [dict(zip(keys, row)) for row in rows]
#     for team in players_res:
#         team[
#             "Player"
#         ] = f"<a href = '../tournaments/{str(team['tournament_id']).replace('.0', '')}/player-detail#{slug(team['Player']) + '-' + slug(team['team'])}'>{team['Player']}</a>"

#     cur.execute(
#         f"""SELECT
# game_id,
# team,
# round, total_pts, bonus_pts, bonuses_heard, opp_pts
# from team_games
# LEFT JOIN teams on team_games.team_id = teams.team_id
# where game_id = {game_id}
#     """
#     )

#     rows = cur.fetchall()
#     keys = [k[0] for k in cur.description]
#     teams_res = [dict(zip(keys, row)) for row in rows]

# res = {
#     # "Summary": summary_res,
#     "Players": players_res,
#     # "Teams": teams_res
#     }

# db.collection("games").document(game_id).update(res)
# print(game_id)
