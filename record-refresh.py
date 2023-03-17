import pandas as pd
import sqlite3 as sq
import utils

records = []
con = sq.connect("stats.db")
cur = con.cursor()

# School Most Wins
cur.execute(f"""
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for tournament in res:
        if tournament['slug']:
            tournament['School'] = f"<a href = '../schools/{tournament['slug']}'>{tournament['School']}</a>"

records.append(res)

    # School Highest Winning Percentage
cur.execute(f"""
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
records.append([dict(zip(keys, row)) for row in rows])

    # Most Tournaments Won
cur.execute(f"""
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for tournament in res:
        if tournament['slug']:
                tournament['School'] = f"<a href = '../schools/{tournament['slug']}'>{tournament['School']}</a>"

records.append(res)

    # Nats titles
cur.execute(f"""SELECT champions.*, schools.slug from champions
left join teams on champions.team_id = teams.team_id
left join schools on teams.school_id = schools.school_id""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for tournament in res:
        if tournament['slug']:
                tournament['school'] = f"<a href = '../schools/{tournament['slug']}'>{tournament['school']}</a>"
df = pd.DataFrame(res).groupby(
['tournament', 'school']
)['year'].agg(
'count'
).reset_index().pivot(
columns='tournament',
values='year',
index='school'
).reset_index().fillna(0).assign(
Total=lambda x: x['ACF Nationals'] + x['DI ICT']
).sort_values(
['Total'], ascending=False
)

records.append(df.to_dict('records'))

# Most Team Wins in a Season
cur.execute(f"""
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['Season'] = f"<a href = '../seasons/{en['Season']}'>{en['Season']}</a>"

records.append(res)

# Most Team Wins in a Season
cur.execute(f"""
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['Season'] = f"<a href = '../seasons/{en['Season']}'>{en['Season']}</a>"

records.append(res)

# Highest National Tournament Team PP20TUH
cur.execute(f"""
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
LIMIT 10)""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Team'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

records.append(res)

# Highest National Tournament Team TU%
cur.execute(f"""
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
LIMIT 10)""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Team'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

records.append(res)

# Highest National Tournament PPB
cur.execute(f"""
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
GROUP BY 1, 2, 3, 4, 5, 6""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
df = pd.DataFrame([dict(zip(keys, row)) for row in rows])
df_mean = df.groupby(['Season', 'Tournament'])['PPB'].agg(
'mean'
).reset_index().rename(columns={'PPB': 'mean'})
df_sd = df.groupby(['Season', 'Tournament'])['PPB'].agg(
'std'
).reset_index().rename(columns={'PPB': 'sd'})
df = df.merge(df_mean, on=['Season', 'Tournament'])
df = df.merge(df_sd, on=['Season', 'Tournament'])
df['z'] = round(((df['PPB'] - df['mean'])/df['sd']), 2)
df = df.sort_values('z', ascending=False).nlargest(10, 'z')
df[['mean', 'sd', 'z']] = df[['mean', 'sd', 'z']].apply(
lambda x: round(x, 2))
res = df.to_dict('records')
for en in res:
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Team'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

records.append(res)

# Highest Tournament Team PP20TUH
cur.execute(f"""
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
LIMIT 10)""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Team'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

records.append(res)

# Highest Tournament Team TU%
cur.execute(f"""
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
LIMIT 10)""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Team'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

records.append(res)

# Highest Tournament PPB
cur.execute(f"""
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
GROUP BY 1, 2, 3, 4, 5, 6""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
df = pd.DataFrame([dict(zip(keys, row)) for row in rows])
df_mean = df.groupby(['Season', 'Tournament'])['PPB'].agg(
'mean'
).reset_index().rename(columns={'PPB': 'mean'})
df_sd = df.groupby(['Season', 'Tournament'])['PPB'].agg(
'std'
).reset_index().rename(columns={'PPB': 'sd'})
df = df.merge(df_mean, on=['Season', 'Tournament'])
df = df.merge(df_sd, on=['Season', 'Tournament'])
df['z'] = round(((df['PPB'] - df['mean'])/df['sd']), 2)
df = df.sort_values('z', ascending=False).nlargest(10, 'z')
df[['mean', 'sd', 'z']] = df[['mean', 'sd', 'z']].apply(
lambda x: round(x, 2))
res = df.to_dict('records')
for en in res:
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Team'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

records.append(res)

# Most Points in a Game, Winning Team
cur.execute(f"""
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Team'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"
        en['Pts'] = f"<a href = '../games/{utils.string(en['game_id'])}'>{round(en['Pts'])}</a>"

records.append(res)

# Most PP20TUH in a Full Game, Winning Team
cur.execute(f"""
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Team'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"
        en['PP20TUH'] = f"<a href = '../games/{utils.string(en['game_id'])}'>{en['PP20TUH']}</a>"

records.append(res)

# Most PP20TUH in a Full Game, Winning Team
cur.execute(f"""
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['Score'] = en['Score'].replace('.0', '')
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Pts'] = f"<a href = '../games/{utils.string(en['game_id'])}'>{round(en['Pts'])}</a>"

records.append(res)

# Most PP20TUH in a Game, Both Teams
cur.execute(f"""
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['Score'] = en['Score'].replace('.0', '')
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['PP20TUH'] = f"<a href = '../games/{utils.string(en['game_id'])}'>{en['PP20TUH']}</a>"

records.append(res)

# Most Negs in a Game, Winning Team
cur.execute(f"""
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Team'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"
        en['-5'] = f"<a href = '../games/{utils.string(en['game_id'])}'>{round(en['-5'])}</a>"

records.append(res)


# Most Negs in a Game, Winning Team
cur.execute(f"""
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Team'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"
        en['PPB'] = f"<a href = '../games/{utils.string(en['game_id'])}'>{en['PPB']}</a>"

records.append(res)

# Grails
cur.execute(f"""
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
ORDER BY Pts desc""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Team'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"
        en['Pts'] = f"<a href = '../games/{utils.string(en['game_id'])}'>{round(en['Pts'])}</a>"

records.append(res)


        # Most Player Points Scored
cur.execute(f"""
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
LIMIT 10)""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        if en['slug']:
                en['Player'] = f"<a href = '../players/{en['slug']}'>{en['Player']}</a>"

records.append(res)

# Most Player Tournaments Played
cur.execute(f"""SELECT fname || ' ' || lname as Player, slug,
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for entry in res:
        if entry['slug']:
                entry['Player'] = f"<a href = '../players/{entry['slug']}'>{entry['Player']}</a>"

records.append(res)

# Most Player National Tournaments Played
cur.execute(f"""SELECT fname || ' ' || lname as Player, slug,
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for entry in res:
        if entry['slug']:
                entry['Player'] = f"<a href = '../players/{entry['slug']}'>{entry['Player']}</a>"

records.append(res)

# Most Player Wins
cur.execute(f"""SELECT fname || ' ' || lname as Player, slug,
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for entry in res:
        if entry['slug']:
                entry['Player'] = f"<a href = '../players/{entry['slug']}'>{entry['Player']}</a>"

records.append(res)

# Most Tournament Player Wins
cur.execute(f"""
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for entry in res:
        if entry['slug']:
                entry['Player'] = f"<a href = '../players/{entry['slug']}'>{entry['Player']}</a>"

records.append(res)

# Highest Player winning pct
cur.execute(f"""
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for entry in res:
        if entry['slug']:
                entry['Player'] = f"<a href = '../players/{entry['slug']}'>{entry['Player']}</a>"

records.append(res)

# Most Points in a Season
cur.execute(f"""
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for entry in res:
        if entry['player_slug']:
                entry['Player'] = f"<a href = '../players/{entry['player_slug']}'>{entry['Player']}</a>"
        if entry['school_slug']:
                entry['School'] = f"<a href = '../schools/{entry['school_slug']}'>{entry['School']}</a>"

records.append(res)

# Highest PP20TUH in a Season
cur.execute(f""" Select * from (
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for entry in res:
        if entry['player_slug']:
                entry['Player'] = f"<a href = '../players/{entry['player_slug']}'>{entry['Player']}</a>"
        if entry['school_slug']:
                entry['School'] = f"<a href = '../schools/{entry['school_slug']}'>{entry['School']}</a>"

records.append(res)

# Highest PP20TUH in a Season
cur.execute(f""" Select * from (
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for entry in res:
        if entry['player_slug']:
                entry['Player'] = f"<a href = '../players/{entry['player_slug']}'>{entry['Player']}</a>"
        if entry['school_slug']:
                entry['School'] = f"<a href = '../schools/{entry['school_slug']}'>{entry['School']}</a>"

records.append(res)

        # Highest PP20TUH in a Tournament
cur.execute(f""" Select *, printf("%.2f", rawPP20TUH) as PP20TUH from (SELECT sets.year as Season, 
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['PP20TUH'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Player'])}-{utils.slug(en['Team'])}'>{en['PP20TUH']}</a>"
        if en['player_slug']:
                en['Player'] = f"<a href = '../players/{en['player_slug']}'>{en['Player']}</a>"
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Team'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

records.append(res)

# Highest PP20TUH in a National Tournament
cur.execute(f""" Select *, printf("%.2f", rawPP20TUH) as PP20TUH from (SELECT sets.year as Season, 
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['PP20TUH'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Player'])}-{utils.slug(en['Team'])}'>{en['PP20TUH']}</a>"
        if en['player_slug']:
                en['Player'] = f"<a href = '../players/{en['player_slug']}'>{en['Player']}</a>"
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Team'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

records.append(res)

# Most Negs per 20 tuh Tournament
cur.execute(f""" Select * from (SELECT sets.year as Season, 
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['-5P20TUH'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Player'])}-{utils.slug(en['Team'])}'>{en['-5P20TUH']}</a>"
        if en['player_slug']:
                en['Player'] = f"<a href = '../players/{en['player_slug']}'>{en['Player']}</a>"
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Team'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

records.append(res)

# Most Player points in a game
cur.execute(f""" Select * from (SELECT sets.year as Season,
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['Pts'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Player'])}-{utils.slug(en['Team'])}'>{round(en['Pts'])}</a>"
        if en['player_slug']:
                en['Player'] = f"<a href = '../players/{en['player_slug']}'>{en['Player']}</a>"
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Team'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

records.append(res)

# Most Player points in a national tournament game
cur.execute(f""" Select * from (SELECT sets.year as Season,
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['Pts'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Player'])}-{utils.slug(en['Team'])}'>{round(en['Pts'])}</a>"
        if en['player_slug']:
                en['Player'] = f"<a href = '../players/{en['player_slug']}'>{en['Player']}</a>"
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Team'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

records.append(res)

# Most Tossups game
cur.execute(f""" Select * from (SELECT sets.year as Season,
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['Tossups'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Player'])}-{utils.slug(en['Team'])}'>{round(en['Tossups'])}</a>"
        if en['player_slug']:
                en['Player'] = f"<a href = '../players/{en['player_slug']}'>{en['Player']}</a>"
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Team'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

records.append(res)

# Most Negs game
cur.execute(f""" Select * from (SELECT sets.year as Season,
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for en in res:
        en['-5'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Player'])}-{utils.slug(en['Team'])}'>{round(en['-5'])}</a>"
        if en['player_slug']:
                en['Player'] = f"<a href = '../players/{en['player_slug']}'>{en['Player']}</a>"
        en['Tournament'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}'>{en['Tournament']}</a>"
        en['Team'] = f"<a href = '../tournaments/{utils.string(en['tournament_id'])}#{utils.slug(en['Team'])}'>{en['Team']}</a>"

records.append(res)

# Most Tournaments Hosted
cur.execute(f"""SELECT school as School, slug,
count(distinct tournaments.tournament_id) as Tournaments
from tournaments
left join sites on tournaments.site_id = sites.site_id
left join schools on sites.school = schools.school_name
where school is not null
GROUP BY 1, 2
ORDER BY 3 desc
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for entry in res:
        if entry['slug']:
                entry['School'] = f"<a href = '../schools/{entry['slug']}'>{entry['School']}</a>"

records.append(res)

# Most Tournaments Hosted
cur.execute(f"""SELECT 
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
LIMIT 10""")

rows = cur.fetchall()
keys = [k[0] for k in cur.description]
res = [dict(zip(keys, row)) for row in rows]
for entry in res:
        entry['Teams'] = f"<a href = '../tournaments/{utils.string(entry['tournament_id'])}'>{entry['Teams']}</a>"
        if entry['slug']:
                entry['Host'] = f"<a href = '../schools/{entry['slug']}'>{entry['Host']}</a>"

records.append(res)

import pickle

def save_object(obj, filename):
        with open(filename, 'wb') as outp:  # Overwrites any existing file.
                pickle.dump(obj, outp, pickle.HIGHEST_PROTOCOL)

# sample usage
save_object(records, 'record-book.pkl')
