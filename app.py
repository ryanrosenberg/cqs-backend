import os
from flask import Flask
from flask_cors import CORS
from flask_restful import Api
from resources.players import PlayerList, Player
from resources.tournaments import RecentTournaments, TournamentList, Tournament
from resources.teams import TeamList, Team, TeamsThisYear
from resources.sets import AllEditors, SetList, Set
from resources.records import Records
from resources.circuits import CircuitList, Circuit
from resources.games import GamesList, Game
from resources.seasons import SeasonList, Season, AllSeasons


app = Flask(__name__)
app.url_map.strict_slashes = False
CORS(app)
api = Api(app)

api.add_resource(PlayerList, '/players', '/players/')
api.add_resource(Player, '/players/<player_slug>')

api.add_resource(TeamList, '/teams', '/teams/')
api.add_resource(Team, '/teams/<team_slug>')

api.add_resource(RecentTournaments, '/tournaments/recent')
api.add_resource(TournamentList, '/tournaments/')
api.add_resource(Tournament, '/tournaments/<tournament_id>')
api.add_resource(TeamsThisYear, '/teams/thisyear')

api.add_resource(GamesList, '/games', '/games/')
api.add_resource(Game, '/games/<game_id>')

api.add_resource(AllEditors, '/sets/editors')
api.add_resource(Set, '/sets/<set_slug>')
api.add_resource(SetList, '/sets', '/sets/')

api.add_resource(Records, '/records', '/records/')

api.add_resource(CircuitList, '/circuits', '/circuits/')
api.add_resource(Circuit, '/circuits/<string:circuit_slug>/')

api.add_resource(SeasonList, '/seasons', '/seasons/')
api.add_resource(AllSeasons, '/seasons/champions', '/seasons/champions/')
api.add_resource(Season, '/seasons/<season>/')

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))