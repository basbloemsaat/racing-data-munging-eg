#!/usr/bin/env python

from rdmdb import exec_query, save_to_json


from pprint import pprint
from dumper import dump

races = exec_query(
    query='''
        SELECT year, round
        FROM races
        WHERE year>=2016
    '''
)

for race in races:
    pprint(race)

    exec_query(
        query = '''
            SELECT
                dr.driverRef
                , dr.forename
                , dr.surname
                , co.constructorRef
                , co.name as constructor
                , re.grid
                , re.position
                , re.laps as laps_run
                , st.status
                , lt.lap as lap_nr
                , lt.position as lap_position
                , lt.time as lap_time
                , lt.milliseconds as lap_ms
                , ps.stop as stop_nr
                , ps.duration as stop_duration
                , ps.milliseconds as stop_ms

            FROM
                results re
                JOIN races ra ON ra.raceId = re.raceId
                JOIN drivers dr ON dr.driverId = re.driverId
                JOIN constructors co ON co.constructorId = re.constructorId
                LEFT JOIN laptimes lt ON lt.raceId = re.raceId AND lt.driverId = re.driverId
                JOIN status st ON st.statusId = re.statusId
                LEFT JOIN pitstops ps ON ps.raceId = re.raceId  AND ps.driverId = re.driverId AND ps.lap = lt.lap
            WHERE year = ? AND round = ?
            ORDER BY re.grid IS NULL, re.grid ASC
        ''',
        args=[race['year'], race['round']],
        json_file='races/' + str(race['year']) + '_' +  str(race['round']) +'_laps.json'

    )

