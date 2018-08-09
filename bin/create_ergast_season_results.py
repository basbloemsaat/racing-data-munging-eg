#!/usr/bin/env python

from rdmdb import exec_query, save_to_json


from pprint import pprint
from dumper import dump


seasons = exec_query(
    query='''
        SELECT
            ra.year
            , count(DISTINCT d.driverId) as winners
            , count(DISTINCT ra.raceId) as races
            , s.url
        FROM
            seasons s
            JOIN races ra ON ra.year=s.year
            LEFT JOIN results re ON re.raceId = ra.raceId AND re.position=1
            LEFT JOIN drivers d ON d.driverId = re.driverId
        GROUP BY ra.year
        ORDER BY ra.year
    '''
)


for season in seasons:
    season_year = season['year']
    results = {}

    results['qualifying'] = exec_query(
        query='''
            SELECT
                d.driverRef
                , d.forename || ' ' || d.surname AS driverName
                , co.constructorRef
                , co.name AS constructorName
                , ci.country AS raceCountry
                , ra.round
                , ra.date
                , MIN(

                CASE
                    WHEN q.q1='' THEN 'no time'
                    WHEN q.q1 IS NULL THEN 'no time'
                    ELSE q.q1
                END
                ,
                CASE
                    WHEN q.q2='' THEN 'x'
                    WHEN q.q2 IS NULL THEN 'x'
                    ELSE q.q2
                END
                ,
                CASE
                    WHEN q.q3='' THEN 'x'
                    WHEN q.q3 IS NULL THEN 'x'
                    ELSE q.q3
                END

                ) AS LapTime
                , q.position
            FROM
                qualifying q
                JOIN races ra ON ra.raceId = q.raceId
                JOIN drivers d ON d.driverId = q.driverId
                JOIN constructors co ON co.constructorId = q.constructorId
                JOIN circuits ci ON ci.circuitId = ra.circuitId

            WHERE ra.year = ?
            ORDER BY ra.round ASC, q.position ASC
        '''
        , args=[season_year]
    )


    results['race'] =exec_query(
        query='''
            SELECT 
                d.driverRef
                , d.forename || ' ' || d.surname AS driverName
                , co.constructorRef
                , co.name AS constructorName
                , ci.country AS raceCountry
                , ra.round
                , ra.date
                , r.position
                , s.status
                , r.time
                , r.milliseconds
            FROM
                results r
                JOIN races ra ON ra.raceId = r.raceId
                JOIN drivers d ON d.driverId = r.driverId
                JOIN constructors co ON co.constructorId = r.constructorId
                JOIN circuits ci ON ci.circuitId = ra.circuitId
                JOIN status s ON s.statusId = r.statusId
            WHERE ra.year = ?
            ORDER BY  ra.year, ra.round, r.position IS NULL ASC
        '''
        , args=[season_year]
    )

    save_to_json(results, 'seasons/' +
                 str(season_year) + '_results.json')

