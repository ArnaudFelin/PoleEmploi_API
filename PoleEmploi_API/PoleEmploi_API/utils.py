
import json
import re

#=====================================================================================================================================

#A conversion module for googletrans
def format_json(original):
    try:
        converted = json.loads(original)
    except ValueError:
        converted = legacy_format_json(original)

    return converted

def legacy_format_json(original):
    # save state
    states = []
    text = original

    # save position for double-quoted texts
    for i, pos in enumerate(re.finditer('"', text)):
        # pos.start() is a double-quote
        p = pos.start() + 1
        if i % 2 == 0:
            nxt = text.find('"', p)
            states.append((p, text[p:nxt]))

    # replace all wiered characters in text
    while text.find(',,') > -1:
        text = text.replace(',,', ',null,')
    while text.find('[,') > -1:
        text = text.replace('[,', '[null,')

    # recover state
    for i, pos in enumerate(re.finditer('"', text)):
        p = pos.start() + 1
        if i % 2 == 0:
            j = int(i / 2)
            nxt = text.find('"', p)
            # replacing a portion of a string
            # use slicing to extract those parts of the original string to be kept
            text = text[:p] + states[j][1] + text[nxt:]

    converted = json.loads(text)
    return converted

#=====================================================================================================================================

def my_logger(logger, msg, log_type = 'default', log_title = ''):
    
    if log_type == 'default':
        logger(f'=====> {msg}')                   
    elif log_type == 'title':
        print(f'\n===================================== {log_title} ===============================================================')
        logger(msg)
        print('============================================================================================================\n')

#=====================================================================================================================================

def my_logger_debug(logger, msg, debug_log_sub_level = 0, debug_sub_level = 0):
    
    if debug_log_sub_level == 0 and debug_sub_level == 0:
        print(f'\n################################### DEBUG {debug_sub_level} ###############################################################')
        logger(msg)
        print('############################################################################################################\n')
    elif debug_log_sub_level == 1 and (debug_sub_level == 0 or debug_sub_level == 1):
        print(f'\n################################### DEBUG {debug_sub_level} ###############################################################')
        logger(msg)
        print('############################################################################################################\n')
