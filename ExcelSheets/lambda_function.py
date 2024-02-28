import go_api
import csv
import json
import datetime
import dateutil.parser as parser
import requests
import pandas as pd
from pandas import ExcelWriter
import boto3

print("Preparing to load data...")
targetTenant = "https://extremeffl.123app.io"
tenantAPIKey = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIiwiaXNzIjoiZXh0cmVtZWZmbCIsImdlbmVyYXRlZERhdGUiOjE2OTE0MjUxNTB9.QiTmn81OWd7hCzp11Q4maz1auTbo_hBQai5oSnP0WkI"
sportsDataKey = "81a57f3c833d4be4b7fbf0be8b0ced8f"
sportsDataKey = "b46bceddcc8a41f1be7ee335677f5176"

targets = ["NFL_Players", "Player", "PlayerID"]
targetEntity = targets[0]
targetView = targets[1]
uniqueFieldName = targets[2]
add_data = True
update_data = True
execute_flag = True
lookups = []


def array_contains(array, field, value):
    for row in array:
        if str(row[field]) == str(value):
            return row


# store local references to lookup
def lookup_entity_values(entity, view):
    readRequestJSON = json.dumps({"request": {"operationType": "read"}})
    go_entity_values = {}
    go_api.get_go_data(targetTenant, entity, view, readRequestJSON, tenantAPIKey, go_entity_values, 0)
    go_values = []
    for key, value in go_entity_values.items():
        go_values.append(value)
    lookups.append({"entity": entity, "value": go_values})
    # lookups.append({"entity":entity, go_entity_values})


def get_entity_value(entity, field, value):
    if value.isnumeric():
        value = float(value)
    entity_ref = array_contains(lookups, "entity", entity)
    value_ref = array_contains(entity_ref["value"], field, value)
    if value_ref is not None:
        return value_ref["_id"]


def get_game_week(check_date):
    if type(check_date) is datetime.date:
        check_date_week = check_date.strftime("%V")
        season_start = datetime.date(2023, 9, 10)
        season_start_week = season_start.strftime("%V")
        season_current_week = int(check_date_week) - int(season_start_week) + 1
        if check_date.isocalendar()[2] < 2:
            season_current_week -= 1
        return season_current_week
    else:
        return -1


# confirm path to file is defined
def lambda_handler(event, context):
    print("Starting run")
    # DEFINE VARIABLES
    s3 = boto3.client('s3')
    bucket = 'efflfiles'
    # today = datetime.date.today()
    # timestamp_EST = str(datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-4), 'EST')))[0:16]
    # current_game_week = get_game_week(today) # number, this is used when calling the SportsData APIs to define the week of stats to pull
    timestamp_EST = str(datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-5), 'EST')))[0:10]
    est_date = datetime.datetime.strptime(timestamp_EST, "%Y-%m-%d").date()
    current_game_week = get_game_week(
        est_date)  # number, this is used when calling the SportsData APIs to define the week of stats to pull
    timestamp_EST = str(datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-5), 'EST')))[0:16]
    last_week = current_game_week - 1  # used to request lineups from previous week
    season = "2023REG"  # Examples: 2015REG, 2015PRE, 2015POST
    filename = 'ExtremeFFLScoresExport_' + season + '_' + str(current_game_week) + '_' + str(timestamp_EST) + '.xlsx'
    file_path = f"/tmp/{filename}"
    writer = ExcelWriter(file_path)
    nfl_teams_used = []  # local array of  nfl teams that were used in lineups
    nflTeamsInUseReadJSON = {"request": {"operationType": "read", "filter": {"criteria": [], "operator": "or"}}}

    nfl_players_used = []  # local array of nfl players that were used in lineups
    nflPlayersInUseReadJSON = {"request": {"operationType": "read", "filter": {"criteria": [], "operator": "or"}}}
    # Get all the lineups from this week
    lineups = {}
    lineupsReadJSON = {"request": {"operationType": "read", "filter": {
        "criteria": [{"fieldName": "Week", "operator": "equals", "value": current_game_week}], "operator": "and"}}}
    go_api.get_go_data(targetTenant, "EFF_Team_Lineups", "Lineup", json.dumps(lineupsReadJSON), tenantAPIKey, lineups,
                       0)
    local_lineups = []
    for key, value in lineups.items():
        local_lineups.append(value)
    for lineup in local_lineups:
        # Add players to local array
        player_fields = ["Quarterback", "Running_Back1", "Running_Back2", "Wide_Receiver1", "Wide_Receiver2",
                         "Tight_End", "Kicker"]
        for position in player_fields:
            if position in lineup:
                if type(lineup[position]).__name__ != "NoneType":
                    player_id = int(lineup[position])
                    points_property_name = position + "_Points"
                    player_points = 0
                    if points_property_name in lineup:
                        if type(lineup[points_property_name]).__name__ != "NoneType":
                            player_points = int(lineup[points_property_name])
                    existing_player = array_contains(nfl_players_used, "player_id", int(player_id))
                    if existing_player is None:
                        nfl_players_used.append(
                            {"player_id": int(player_id), "SportsDataPlayerID": 0, "Points": player_points})
                        nflPlayersInUseReadJSON["request"]["filter"]["criteria"].append(
                            {"fieldName": "_id", "operator": "equals", "value": player_id})
        # Add Teams to local array
        team_fields = ["Offense", "Defense"]
        for team_field in team_fields:
            if team_field in lineup:
                if lineup[team_field]:
                    team_id = float(lineup[team_field])
                    offense_points = 0
                    defense_points = 0
                    points_property_name = team_field + "_Points"
                    if points_property_name in lineup:
                        team_points = int(lineup[points_property_name])
                        if team_field == "Offense":
                            offense_points = team_points
                        else:
                            defense_points = team_points
                    existing_team = array_contains(nfl_teams_used, "team_id", team_id)
                    if existing_team is None:
                        nfl_teams_used.append(
                            {"team_id": team_id, "SportsDataTeamID": 0, "OffensePoints": offense_points,
                             "DefensePoints": defense_points})
                        nflTeamsInUseReadJSON["request"]["filter"]["criteria"].append(
                            {"fieldName": "_id", "operator": "equals", "value": team_id})
    # Get all the NFL players from the lineups from this week (so that
    print("Retrieving players from 123Go")
    nfl_players_dict = {}
    go_api.get_go_data(targetTenant, "NFL_Players", "Player", json.dumps(nflPlayersInUseReadJSON), tenantAPIKey,
                       nfl_players_dict, 0)
    local_nfl_players = []
    for key, value in nfl_players_dict.items():
        local_nfl_players.append(value)
    print(local_nfl_players)
    for nfl_player in nfl_players_used:
        local_nfl_player = array_contains(local_nfl_players, "_id", int(nfl_player["player_id"]))
        if local_nfl_player:
            nfl_player["SportsDataPlayerID"] = int(local_nfl_player["PlayerID"])
            player_TeamID = local_nfl_player["TeamID"]
            criteriaObject = {"fieldName": "TeamID", "operator": "equals", "value": float(player_TeamID)}
            if criteriaObject not in nflTeamsInUseReadJSON["request"]["filter"]["criteria"]:
                nflTeamsInUseReadJSON["request"]["filter"]["criteria"].append(criteriaObject)

    # Get all the NFL teams from the lineups from this week
    print("Retrieving teams from 123Go")
    nfl_teams_dict = {}
    go_api.get_go_data(targetTenant, "NFL_Teams", "Team", json.dumps(nflTeamsInUseReadJSON), tenantAPIKey,
                       nfl_teams_dict, 0)
    local_nfl_teams = []
    for key, value in nfl_teams_dict.items():
        local_nfl_teams.append(value)
    for nfl_team in nfl_teams_used:
        local_nfl_team = array_contains(local_nfl_teams, "_id", int(nfl_team["team_id"]))
        if local_nfl_team:
            nfl_team["SportsDataTeamID"] = int(local_nfl_team["TeamID"])
            nfl_team["Key"] = local_nfl_team["Key"]
    # Get SportsData.io data

    # Teams
    print("Retrieving stats from SportsData.io")
    team_stat_lines = []
    team_stats_url = 'https://api.sportsdata.io/v3/nfl/scores/json/TeamGameStats/%s/%s?key=%s' % (
    season, current_game_week, sportsDataKey)

    team_stats = requests.get(team_stats_url)
    if team_stats.status_code == 200:
        team_stats_json = json.loads(team_stats.content)
        for team in team_stats_json:
            team_stat_line = {}
            win_loss = team["OpponentScore"] - team["Score"]
            if win_loss < 0:
                win_loss = 5
            else:
                win_loss = -5
            team_stat_line["Team"] = team["Team"]
            team_stat_line["TeamID"] = team["TeamID"]
            team_stat_line["Opponent"] = team["Opponent"]
            team_stat_line["GameDate"] = team["Date"]
            team_stat_line["OFF_PointsScored"] = team["Score"]
            team_stat_line["OFF_TotalYards"] = team["OffensiveYards"]
            team_stat_line["OFF_WinLoss"] = win_loss
            team_stat_line["OFF_SpecialTeamTDs"] = team["PuntReturnTouchdowns"] + team["KickReturnTouchdowns"]
            team_stat_line["OFF_FinalPointsScore"] = (team["Score"] - 21) * 2
            team_stat_line["OFF_TotalYardsScore"] = (team["OffensiveYards"] - 340) / 10
            team_stat_line["OFF_WinLossScore"] = win_loss
            team_stat_line["OFF_SpecialTeamsScore"] = team_stat_line["OFF_SpecialTeamTDs"] * 6
            team_stat_line["OFF_FinalScore"] = team_stat_line["OFF_FinalPointsScore"] + team_stat_line[
                "OFF_TotalYardsScore"] + team_stat_line["OFF_WinLossScore"] + team_stat_line["OFF_SpecialTeamsScore"]
            team_stat_line["DEF_PointsAgainst"] = team["OpponentScore"]
            team_stat_line["DEF_SacksPlusTurnovers"] = team["Sacks"] + team["OpponentFumblesLost"] + team[
                "OpponentPassingInterceptions"]
            team_stat_line["DEF_WinLoss"] = win_loss
            team_stat_line["DEF_DefensiveTDs"] = team["FumbleReturnTouchdowns"] + team["InterceptionReturnTouchdowns"] + \
                                                 team["BlockedKickReturnTouchdowns"]
            team_stat_line["DEF_Safeties"] = team["Safeties"]
            team_stat_line["DEF_FinalPointsScore"] = (team["OpponentScore"] - 21) * -2
            team_stat_line["DEF_SacksPlusTurnoversScore"] = team_stat_line["DEF_SacksPlusTurnovers"]
            team_stat_line["DEF_WinLossScore"] = win_loss
            team_stat_line["DEF_DefensiveTDsPlusSafetyScore"] = (team_stat_line["DEF_DefensiveTDs"] * 6) + \
                                                                team_stat_line["DEF_Safeties"]
            team_stat_line["DEF_FinalScore"] = team_stat_line["DEF_FinalPointsScore"] + team_stat_line[
                "DEF_SacksPlusTurnoversScore"] + team_stat_line["DEF_WinLossScore"] + team_stat_line[
                                                   "DEF_DefensiveTDsPlusSafetyScore"]
            local_nfl_team = array_contains(nfl_teams_used, "SportsDataTeamID", team["TeamID"])
            if type(local_nfl_team).__name__ != "NoneType":
                local_nfl_team["DefensePoints"] = team_stat_line["DEF_FinalScore"]
                local_nfl_team["OffensePoints"] = team_stat_line["OFF_FinalScore"]

            team_stat_lines.append(team_stat_line)
    # Players
    print("Retrieving player stats from SportsData.io")
    team_player_stat_lines = []
    team_kicker_stat_lines = []
    for team in local_nfl_teams:
        player_stats_url = 'https://api.sportsdata.io/v3/nfl/stats/json/PlayerGameStatsByTeam/%s/%s/%s?key=%s' % (
        season, current_game_week, team["Key"], sportsDataKey)
        player_stats = requests.get(player_stats_url)
        if player_stats.status_code == 200:
            players_stats_json = json.loads(player_stats.content)
            for player in players_stats_json:
                if player["Played"] == 1 and player["Position"] in ["RB", "WR", "TE"]:
                    player_stat_line = {}
                    player_stat_line["Team"] = player["Team"]
                    player_stat_line["PlayerName"] = player["Name"]
                    player_stat_line["PlayerID"] = player["PlayerID"]
                    player_stat_line["Position"] = player["Position"]
                    player_stat_line["SportsDataPlayerID"] = int(player["PlayerID"])
                    player_stat_line["Passing"] = player["PassingYards"]
                    player_stat_line["Rushing"] = player["RushingYards"]
                    player_stat_line["Receiving"] = player["ReceivingYards"]
                    player_stat_line["TotalYards"] = player_stat_line["Receiving"] + player_stat_line["Rushing"] + \
                                                     player_stat_line["Passing"]
                    player_stat_line["TotalTDs"] = player["RushingTouchdowns"] + player["PassingTouchdowns"] + player[
                        "ReceivingTouchdowns"]
                    player_stat_line["TwoPtConversions"] = player["TwoPointConversionPasses"] + player[
                        "TwoPointConversionRuns"] + player["TwoPointConversionReceptions"]
                    player_stat_line["Turnovers"] = player["FumblesLost"] + player["PassingInterceptions"]
                    player_stat_line["YardScore"] = player_stat_line["TotalYards"] / 3
                    player_stat_line["TDScore"] = player_stat_line["TotalTDs"] * 6
                    player_stat_line["TwoPtConversionScore"] = player_stat_line["TwoPtConversions"] * 2
                    player_stat_line["TurnoverScore"] = player_stat_line["Turnovers"] * -3
                    player_stat_line["FinalScore"] = round(
                        player_stat_line["YardScore"] + player_stat_line["TDScore"] + player_stat_line[
                            "TwoPtConversionScore"] + player_stat_line["TurnoverScore"], 1)
                    team_player_stat_lines.append(player_stat_line)
                    local_nfl_player = array_contains(nfl_players_used, "SportsDataPlayerID", int(player["PlayerID"]))
                    if local_nfl_player is not None:
                        local_nfl_player["Points"] = player_stat_line["FinalScore"]
                elif player["Played"] == 1 and player["Position"] == "QB":
                    player_stat_line = {}
                    player_stat_line["Team"] = player["Team"]
                    player_stat_line["PlayerName"] = player["Name"]
                    player_stat_line["PlayerID"] = player["PlayerID"]
                    player_stat_line["Position"] = player["Position"]
                    player_stat_line["SportsDataPlayerID"] = int(player["PlayerID"])
                    player_stat_line["Passing"] = player["PassingYards"]
                    player_stat_line["Rushing"] = player["RushingYards"]
                    player_stat_line["Receiving"] = player["ReceivingYards"]
                    player_stat_line["TotalYards"] = player_stat_line["Receiving"] + player_stat_line["Rushing"] + \
                                                     player_stat_line["Passing"]
                    player_stat_line["TotalTDs"] = player["RushingTouchdowns"] + player["PassingTouchdowns"] + player[
                        "ReceivingTouchdowns"]
                    player_stat_line["TwoPtConversions"] = player["TwoPointConversionPasses"] + player[
                        "TwoPointConversionRuns"] + player["TwoPointConversionReceptions"]
                    player_stat_line["Turnovers"] = player["FumblesLost"] + player["PassingInterceptions"]
                    player_stat_line["YardScore"] = player_stat_line["TotalYards"] / 10
                    player_stat_line["TDScore"] = player_stat_line["TotalTDs"] * 6
                    player_stat_line["TwoPtConversionScore"] = player_stat_line["TwoPtConversions"] * 2
                    player_stat_line["TurnoverScore"] = player_stat_line["Turnovers"] * -3
                    player_stat_line["FinalScore"] = round(
                        player_stat_line["YardScore"] + player_stat_line["TDScore"] + player_stat_line[
                            "TwoPtConversionScore"] + player_stat_line["TurnoverScore"], 1)
                    team_player_stat_lines.append(player_stat_line)
                    local_nfl_player = array_contains(nfl_players_used, "SportsDataPlayerID", int(player["PlayerID"]))
                    if local_nfl_player:
                        local_nfl_player["Points"] = player_stat_line["FinalScore"]
                elif player["Played"] == 1 and player["Position"] == "K":
                    kicker_stat_line = {}
                    kicker_stat_line["Team"] = player["Team"]
                    kicker_stat_line["PlayerName"] = player["Name"]
                    kicker_stat_line["PlayerID"] = player["PlayerID"]
                    kicker_stat_line["Position"] = player["Position"]
                    kicker_stat_line["SportsDataPlayerID"] = int(player["PlayerID"])
                    kicker_stat_line["FGMLessThan30"] = player["FieldGoalsMade0to19"] + player["FieldGoalsMade20to29"]
                    kicker_stat_line["FGM30to39"] = player["FieldGoalsMade30to39"]
                    kicker_stat_line["FGM40to49"] = player["FieldGoalsMade40to49"]
                    kicker_stat_line["FGMGreaterThan49"] = player["FieldGoalsMade50Plus"]
                    kicker_stat_line["ExtraPoints"] = player["ExtraPointsMade"]
                    kicker_stat_line["Misses"] = (player["FieldGoalsMade"] - player["FieldGoalsAttempted"]) + (
                                player["ExtraPointsMade"] - player["ExtraPointsAttempted"])
                    kicker_stat_line["FGPointsScore"] = (kicker_stat_line["FGMLessThan30"] * 3) + (
                                kicker_stat_line["FGM30to39"] * 4) + (kicker_stat_line["FGM40to49"] * 5) + (
                                                                    kicker_stat_line["FGMGreaterThan49"] * 6)
                    kicker_stat_line["XPPointsScore"] = kicker_stat_line["ExtraPoints"]
                    kicker_stat_line["MissedKicksScore"] = kicker_stat_line["Misses"]
                    kicker_stat_line["FinalScore"] = kicker_stat_line["FGPointsScore"] + kicker_stat_line[
                        "XPPointsScore"] + kicker_stat_line["Misses"]
                    team_kicker_stat_lines.append(kicker_stat_line)
                    local_nfl_player = array_contains(nfl_players_used, "SportsDataPlayerID", int(player["PlayerID"]))
                    if local_nfl_player:
                        local_nfl_player["Points"] = kicker_stat_line["FinalScore"]

    # nfl_players_used
    print("233: nfl_players_used: ", nfl_players_used)
    nfl_players_used

    # nfl_teams_used
    print("233: nfl_teams_used: ", nfl_teams_used)

    # local_lineups
    lineupPointsUpdateJSON = {"request": {"operationType": "update", "data": []}}
    for lineup in local_lineups:
        lineup_update_object = {"_id": lineup["_id"]}
        player_fields = ["Quarterback", "Running_Back1", "Running_Back2", "Wide_Receiver1", "Wide_Receiver2",
                         "Tight_End", "Kicker"]
        for player_field in player_fields:
            if player_field in lineup:
                if type(lineup[player_field]).__name__ != "NoneType":
                    player = array_contains(nfl_players_used, "player_id", int(lineup[player_field]))
                    if player is not None:
                        # add to update call with player_id being the 123go _id
                        lineup_points_field = player_field + "_Points"
                        lineup_update_object[lineup_points_field] = player["Points"]

        team_fields = ["Offense", "Defense"]
        for team_field in team_fields:
            if team_field in lineup:
                if type(lineup[team_field]).__name__ != "NoneType":
                    lineup_team = array_contains(nfl_teams_used, "team_id", float(lineup[team_field]))
                    if lineup_team is not None:
                        if lineup_team[team_field + "Points"] != 0:
                            lineup_points_field = team_field + "_Points"
                            lineup_update_object[lineup_points_field] = lineup_team[team_field + "Points"]

        if len(lineup_update_object.items()) > 1:
            lineupPointsUpdateJSON["request"]["data"].append(lineup_update_object)

    # if there are points to allocate, update the system with them now
    if len(lineupPointsUpdateJSON["request"]["data"]) > 1:
        go_api.update_go_data(targetTenant, "EFF_Team_Lineups", "Lineup", json.dumps(lineupPointsUpdateJSON),
                              tenantAPIKey)

    # Prep player data for Excel export
    player_data_frame = pd.DataFrame(team_player_stat_lines)
    player_data_frame.to_excel(writer, 'PlayerStats', index=False)

    # Prep kicker data for Excel export
    kicker_data_frame = pd.DataFrame(team_kicker_stat_lines)
    kicker_data_frame.to_excel(writer, 'KickerStats', index=False)

    # Prep team data for Excel export
    team_data_frame = pd.DataFrame(team_stat_lines)
    team_data_frame.to_excel(writer, 'TeamStats', index=False)

    # Prep lineups data for Excel export
    lineups_data_frame = pd.DataFrame(local_lineups)
    lineups_data_frame.to_excel(writer, 'Lineups', index=False)
    writer.close()
    with open(file_path, "rb") as file:
        s3.put_object(Bucket=bucket, Key=filename, Body=file)