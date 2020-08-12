#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 12 14:01:01 2020

@author: quangkhanh
"""

import re
import unidecode 

from datetime import datetime

def industries_to_set(sector):
    '''
    Read a sector 
    
    Returns 
    -------
          result: set of unique domains in sectors
          
    '''
    
    sector = sector[0]
    filters = "['\[\]]"
    r = map(lambda x: re.sub('[-&(.*)]', '/', x) \
            .replace(r'\r', '').replace(r'\n', '').replace(r'\xa0', '') \
            .split(sep='/'), re.sub(filters, '', sector).split(sep=','))
    result = set() 
    for i in r: 
        for j in i: 
            _j = j.strip().lower()
            result.add(_j) 

    return result
    
def time_to_quarter(time): 
    ''' 
    Convert timestamp to quarter 
    
    Returns 
    -------
        last day, last month of the quarter, the quarter, the year
    
    '''
    
    time = time[0][:10]
    time = datetime.strptime(time, "%Y-%m-%d")
    if time.month < 4: 
        quarter = 1 
        day = 31
        month = 3
    elif time.month < 7: 
        quarter = 2 
        day = 30 
        month = 6 
    elif time.month < 10:
        quarter = 3 
        day = 30
        month = 9 
    else: 
        quarter = 4
        day = 31
        month = 12
    return day, month, quarter, time.year

def academic_level_standardize(level): 
    '''
    Standard job level requirement

    Parameters
    ----------
    level : String

    Returns
    -------
    One of four standard levels: 
        1. Khác 
        2. Trung cấp  
        3. Cao đẳng  
        4. Đại học  

    '''
    try : 
        level = unidecode.unidecode(level.lower())
        if  any(['dai hoc' in level, 
                 'cu nhan' in level, 
                 'ki su' in level, 
                 'tot nghiep' in level]):
            return 'Đại học' 
        
        if any(['trung cap' in level]): 
            return 'Trung cấp'
        
        if any(['cao dang' in level]): 
            return 'Cao đẳng'
    except : 
        pass 
    
    return 'Khác'






