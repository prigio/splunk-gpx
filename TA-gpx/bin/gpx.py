import time
import datetime
import statistics
import json

import gpxpy


gpx_filename = 'Gutemberg_Halbmarathon_Mainz.gpx'

gpx = None




def uctdatetime_to_epoch(datetime_obj):
    if datetime_obj == None:
        return time.time()
    elif datetime_obj.tzinfo is None:
        epoch_dt = datetime.datetime(1970,1,1,0,0,0,0)
        return (datetime_obj - epoch_dt).total_seconds()
    else:
        epoch_dt = datetime.datetime(1970,1,1,0,0,0,0, datetime_obj.tzinfo)
        return (datetime_obj - epoch_dt).total_seconds()


def analyze_file(filename):
    try:
        with open(filename, 'r') as gpx_file:
            gpx = gpxpy.parse(gpx_file)
    except Exception as e:
        print(e)
        return None

    tracks_events = []
    points_events = []

    track_seq = 0
    for track in gpx.tracks:
        track_seq += 1
        md = track.get_moving_data()
        t_event = {
            '_time': uctdatetime_to_epoch(track.get_time_bounds()[0]),
            'sourcetype': "gpx:track",
            'source': gpx_filename,
            'host': gpx.creator,
            'data': dict(
                name = track.name,
                track_seq = track_seq,
                ts = track.get_time_bounds()[0].strftime("%Y-%m-%d %T %z"),
                distance_2d_m = round(track.length_2d(), 3),
                distance_3d_m = round(track.length_3d(), 3),
                duration_s = track.get_duration(),

                delev_downhill_m = round(track.get_uphill_downhill().downhill, 1) if track.has_elevations() else None,
                delev_uphill_m = round(track.get_uphill_downhill().uphill, 1) if track.has_elevations() else None,          
                elev_min_m = track.get_elevation_extremes().minimum if track.has_elevations() else None,
                elev_max_m = track.get_elevation_extremes().maximum if track.has_elevations() else None,

                moving = dict(
                    duration_s = md.moving_time,
                    distance_m = md.moving_distance,
                    avg_speed_mps = round(md.moving_distance / md.moving_time, 3),
                    # speed seconds per KM
                    avg_tempo_spk = round(1/ (md.moving_distance / md.moving_time / 1000), 1)
                ),
                stopped = dict(
                    distance_m = md.stopped_distance,
                    duration_s = md.stopped_time
                )
            )
        }
        
        # arrays used to compute statistics about these sizes    
        tempos = []
        speeds = []
        hearth_rates = []

        prev_p = None
        for segment in track.segments:
            for p in segment.points:
                p_event = {
                 '_time': uctdatetime_to_epoch(p.time),
                 'sourcetype': "gpx:point",
                 'source': gpx_filename,
                 'host': gpx.creator,
                 'data': '%s track_seq=%s lat=%s lon=%s elev=%s' % ( uctdatetime_to_epoch(p.time), 
                    track_seq,
                    p.latitude, 
                    p.longitude, 
                    p.elevation if p.has_elevation() else '')
                }
                
                if len(p.extensions)>0:
                    for ext in p.extensions:
                        for el in list(ext):
                            if el.text is not None and el.text.strip() != '':
                                split = el.tag.split("}")           
                                el_tag = split[1] if len(split)>1 else split[0]
                                try: 
                                    p_event['data'] += ' %s=%s' % (el_tag, float(el.text.strip()))
                                    if el_tag in ('hr', 'heartrate'):
                                        hearth_rates.append(float(el.text.strip()))
                                except: p_event['data'] += ' %s="%s"' % (el_tag, el.text.strip())
                if prev_p is not None:
                    speed_mps = round(p.speed_between(prev_p),6)
                    tempo_spk = round(1/ (p.speed_between(prev_p) / 1000), 1)
                    speeds.append(speed_mps)
                    tempos.append(tempo_spk)
                else:
                    speed_mps = ''
                    tempo_spk = ''
                p_event['data'] += " speed_mps=%s tempo_spk=%s" % (speed_mps, tempo_spk)
                points_events.append(p_event)
                prev_p = p
            
        t_event['data']['stats'] = {}
        if len(hearth_rates) > 0:
            t_event['data']['stats']['hr_avg'] = round(statistics.mean(hearth_rates),0)
            t_event['data']['stats']['hr_stddev'] = round(statistics.stdev(hearth_rates),6)
        else:
            t_event['data']['stats']['hr_avg'] = None
            t_event['data']['stats']['hr_stdev'] = None
        t_event['data']['stats']['speed_avg'] = round(statistics.mean(speeds),3)
        t_event['data']['stats']['speed_stdev'] = round(statistics.stdev(speeds),6)
        t_event['data']['stats']['tempo_avg'] = round(statistics.mean(tempos),1)
        t_event['data']['stats']['tempo_stdev'] = round(statistics.stdev(tempos),6)

        tracks_events.append(t_event)
        return (tracks_events, points_events)

if __name__ == "__main__":
    try:
        tracks_events, points_events = analyze_file(gpx_filename)
        for track in tracks_events:
            print('***SPLUNK*** sourcetype="%s" source="%s" host="%s"' % (track['sourcetype'],track['source'],track['host']))
            print(json.dumps(track['data'], sort_keys=True))
        p = points_events[0]
        print('***SPLUNK*** sourcetype="%s" source="%s" host="%s"' % (p['sourcetype'],p['source'],p['host']))
        for p in points_events:
            print(p['data'])


    except Exception as e:
        pass




                
