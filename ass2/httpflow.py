import yaml
import sys
import os
import schedule
import time
import requests

def invoke(step_id, data, steps):
    step_id = int(step_id)
    step = None
    for i in steps:
        if step_id in i:
            step = i
            break
    step_type = step['type']
    method = step['method']    
    condition = step['condition']
    if step['outbound_url'] == '::input:data':
        outbound_url = data 
    else:
        outbound_url = step['outbound_url']
    response = None
    error = False
    if step_type == 'HTTP_CLIENT':
        try:
            response = requests.request(method, outbound_url)
            error = False
        except:
            error = True
    else:
        print("Error")
    if not error:
        left = condition['if']['equal']['left']
        right = condition['if']['equal']['right']
        thenblock = condition['then']
        elseblock = condition['else']
        if left != 'http.response.code':
            print("Error")
        if left == 'http.response.code' and right == response.status_code:
            perform(thenblock['action'], thenblock['data'], response, steps)
        else:
            perform(elseblock['action'], elseblock['data'], response, steps)
    else:
        perform(elseblock['action'], elseblock['data'], response, steps)

def perform(action, data, response=None, steps=None):
    if action.startswith("::invoke"):
        invoke(action.split(":")[-1], data, steps)
    elif action.startswith("::print") and data =='http.response.headers.content-type':
        print(response.headers['content-type'])
    elif action.startswith("::print") and data =='http.response.headers.X-Ratelimit-Limit':
        print(response.headers['X-Ratelimit-Limit'])
    else:
        print("Error")

def scheduler(pattern, run=None):
    crons = pattern.split()
    minutes = 0
    hours = -1
    weekday = None
    if crons[0] != '*':
        minutes = int(crons[0])
    if crons[1] != '*':
        hours = int(crons[1])
    if crons[2] != '*':
        weekday = int(crons[2])
    if minutes != 0 and hours == -1 and weekday == None:
        schedule.every(minutes).minutes.do(run)    
    elif weekday != None:
        time = '{0:02d}:{1:02d}'.format(hours, minutes)
        if weekday == 0:
            schedule.every().sunday.at(time).do(run)
        elif weekday == 1:
            schedule.every().monday.at(time).do(run)
        elif weekday == 2:
            schedule.every().tuesday.at(time).do(run)
        elif weekday == 3:
            schedule.every().wednesday.at(time).do(run)
        elif weekday == 4:
            schedule.every().thursday.at(time).do(run)
        elif weekday == 5:
            schedule.every().friday.at(time).do(run)
        elif weekday == 6:
            schedule.every().saturday.at(time).do(run)
        else:
            print("Error")
    else:
        time = '{0:02d}:{1:02d}'.format(hours, minutes)
        schedule.every().day.at(time).do(run)

if len(sys.argv) != 2:
    print("Error")
    os.exit(1)

input_file = sys.argv[1]
with open(input_file) as file:
    commands = yaml.load(file, Loader=yaml.FullLoader)
    steps = commands['Steps']
    scheduler = commands['Scheduler']
    pattern = scheduler['when']
    step_id = scheduler['step_id_to_execute']
    step_id = int(step_id[0])
    def wrapper():
        return invoke(step_id, None, steps)
    
    scheduler(pattern, wrapper)
    while True:
        schedule.run_pending()
        time.sleep(1)