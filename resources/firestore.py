import firebase_admin
from firebase_admin import firestore
import sqlite3 as sq

# Application Default credentials are automatically created.
app = firebase_admin.initialize_app()
db = firestore.client()

def slug(str):
    new_str = str.lower().replace(' ', '-')
    return new_str

def string(int):
    new_str = str(int).replace('.0', '')
    return new_str

docs = [doc.id for doc in db.collection(u'players').stream()]

con = sq.connect("stats.db")
cur = con.cursor()

cur.execute("select game_id from games")
player_slugs = [string(r[0]) for r in cur.fetchall() if r[0] is not None and r[0] not in docs]

for game_id in player_slugs:
    print(game_id)
    cur.execute(f"""
        SELECT 
           games.round, teams.team, tournaments.tournament_name, tournaments.tournament_id
           from team_games 
           LEFT JOIN games on team_games.game_id = games.game_id
           LEFT JOIN tournaments on games.tournament_id = tournaments.tournament_id
           LEFT JOIN teams on team_games.team_id = teams.team_id
           LEFT JOIN sets on tournaments.set_id = sets.set_id
           LEFT JOIN sites on tournaments.site_id = sites.site_id
           WHERE team_games.game_id = {game_id}
        """)

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    summary_res = [dict(zip(keys, row)) for row in rows]

        

    cur.execute(f"""SELECT
    tournament_id,
game_id,
team, 
coalesce(fname|| ' ' || lname, player_games.player) as Player, 
ifnull(tuh, 20) as TUH,
powers as \"15\", tens as \"10\", negs as \"-5\", pts as Pts
from player_games
LEFT JOIN teams on player_games.team_id = teams.team_id
LEFT JOIN players on player_games.player_id = players.player_id
LEFT JOIN people on players.person_id = people.person_id
where game_id = {game_id}
    """)

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    players_res = [dict(zip(keys, row)) for row in rows]
    for team in players_res:
        team['Player'] = f"<a href = '../tournaments/{str(team['tournament_id']).replace('.0', '')}#{slug(team['Player']) + '-' + slug(team['team'])}'>{team['Player']}</a>"

    cur.execute(f"""SELECT
game_id,
team,
round, total_pts, bonus_pts, bonuses_heard, opp_pts
from team_games
LEFT JOIN teams on team_games.team_id = teams.team_id
where game_id = {game_id}
    """)

    rows = cur.fetchall()
    keys = [k[0] for k in cur.description]
    teams_res = [dict(zip(keys, row)) for row in rows]

    res = {
        'Summary': summary_res,
        'Players': players_res,
        'Teams': teams_res
    }

    db.collection(u'games').document(game_id).set(res)
