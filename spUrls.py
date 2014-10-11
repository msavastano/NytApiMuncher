import requests
import sys
import json
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pandasql import *
import re
import time

current_milli_time = lambda: int(round(time.time() * 1000))

#http://developer.nytimes.com/docs/article_search_api
    
def dldata_key(b_date, e_date, qu, sort_by, apikey, ran, fqu='source:("The New York Times")'):
    '''
    params:
    b_date : date to start search
    e_date : date to end
    qu : query search string - exact phrases in double quotes
    sort_by : str 'oldest' 'newest'
    apikey : authentication, see API documentation 
    ran : Set to True on initial run to clear out temp file,  False will add on to previous query, not recommended
    fqu : filters by keys - optional - but will ignore any AP, Reuters, etc stories          
    
    '''
    if not os.path.exists('temp'):
        os.makedirs('temp')
        
    if ran == False:
        delFiles = get_file_list()
        for f in delFiles:
            os.remove('temp/'+f)      
        
    
    output = 'temp/'+str(current_milli_time())
    urlnytcc = 'http://api.nytimes.com/svc/search/v2/articlesearch.json?'
    f = open(output+'.txt', "w")
    countercc = 0
    keycc = '&api-key=' + apikey
    urlnytcc= urlnytcc+keycc    
    hits = 1  
    over1009 = False
    while hits > 0 and countercc <= 100:
        #the API retrieves pages of results, 10 per page, gets 1 page per url call
        params2cc = dict(begin_date=b_date, end_date=e_date, q=qu, fq=fqu, sort=sort_by, page=str(countercc))
        r2cc = requests.get(urlnytcc, params=params2cc).text
        
        getHits = json.loads(r2cc)
        if countercc == 0:
            if 'response' in getHits.keys():
                hits = getHits['response']['meta']['hits']
                if hits > 1009:
                    over1009 = True
            else:
                print "EMPTY FILE _ NO HITS"
        hits -= 10      
        
        print >> f, r2cc 
        countercc += 1 
        if over1009 == True:
            if countercc == 101:
                last_date = re.sub(r'[-]', '', getHits['response']['docs'][-1]['pub_date'][0:10])
                dldata_key(last_date, e_date, qu, sort_by, apikey, True, fqu)                
                
    f.close()   
    
    editFiles = get_file_list()
   
    with open('final.txt', 'w') as outfile:
        for fname in editFiles:
            with open('temp/'+fname) as infile:
                for line in infile:
                    outfile.write(line)      
    
def get_file_list():
    dirListing = os.listdir(os.path.realpath('temp'))
    editFiles = []
    for item in dirListing:
        if ".txt" in item:
            editFiles.append(item) 
    return editFiles
    


def createDB(inputFile):
    '''
    creates 7 df's
    
    params:
    inputFile : txt file with json
    
    returns:
    list of 7 df's
    '''
    keyword_dflst = []
    headline_main_dfst = []
    headline_kicker_dfst = []
    person_dfst = []
    byline_dfst = []
    main_dfst = []
    person_article = []
    
    pagescct = open(inputFile)    
    for line2 in pagescct:
        pagescc2 = json.loads(line2)
        if 'response' not in pagescc2.keys():            
            print "EOF"
            break
            
        if len(pagescc2['response']['docs']):
            for key in pagescc2['response']['docs']:            
                
                #print "----KEYWORD TABLE----"
                for word in key['keywords']:                
                    keyword_dflst.append(list( [key['_id'], word['value']] )) 
                    
                #print "----HEADLINE MAIN TABLE----"
                headline_main_dfst.append(list([key['_id'], key['headline']['main']]))
                
                #print "----HEADLINE KICKER TABLE----"
                if 'kicker' in key['headline'].keys():               
                    headline_kicker_dfst.append(list([key['_id'], key['headline']['kicker']]))                 
                
                #print "----PERSON TABLE----" 
                if 'byline' in key.keys():  
                    if type(key['byline']) == type(dict()):
                        if  key['byline'] != None and 'person' in key['byline'].keys():                
                            for byl in key['byline']['person']:
                                if 'lastname' in byl.keys():
                                    temp_list = list()
                                    #temp_list.append(key['_id']) 
                                    temp_list.append(byl['firstname']+byl['lastname']) 
                                    if 'firstname' in byl.keys() and byl['firstname'] != '':                    
                                        temp_list.append(byl['firstname'])
                                    else:
                                        temp_list.append('')
                                    if 'middlename' in byl.keys() and byl['middlename'] != '':                     
                                        temp_list.append(byl['middlename'])
                                    else:
                                        temp_list.append('')
                                    if 'lastname' in byl.keys() and byl['lastname'] != '':                    
                                        temp_list.append(byl['lastname'])
                                    else:
                                        temp_list.append('')
                                    if 'organization' in byl.keys() and byl['organization'] != '':
                                        temp_list.append(byl['organization'])
                                    else:
                                        temp_list.append('')
                                    if 'role' in byl.keys() and byl['role'] != '':
                                        temp_list.append(byl['role'])
                                    else:
                                        temp_list.append('')
                                    if 'rank' in byl.keys() and byl['rank'] != '':
                                       temp_list.append(byl['rank'])
                                    else:
                                        temp_list.append('')
                                    person_dfst.append(temp_list) 
                                    person_article.append(list([byl['firstname']+byl['lastname'], key['_id']]))
                
                #print "----BYLINE TABLE----" 
                if 'byline' in key.keys() and key['byline'] != None:
                    if 'original' in key['byline'] and key['byline']['original'] != '':
                        byline_dfst.append( list([key['_id'], key['byline']['original']]) ) 
                    else:
                        byline_dfst.append(list([key['_id'], '']))            
                
                #print "----MAIN TABLE----" 
                main_dfst.append( list([key['_id'],
                                key['lead_paragraph'],
                                key['web_url'],
                                key['word_count'],
                                key['snippet'],
                                key['abstract'],
                                key['source'],
                                key['pub_date'],
                                key['news_desk'],
                                key['document_type'],
                                key['section_name'],
                                key['subsection_name'],
                                key['print_page'],
                                key['type_of_material']]) )     
                   
    #close(inputFile)
    
    return [pd.DataFrame(person_article, columns=['PERSON_ID','ID']),
               pd.DataFrame(keyword_dflst, columns = ['ID','KEYWORD']),
               pd.DataFrame(headline_kicker_dfst, columns = ['ID','KICKER']),
               pd.DataFrame(headline_main_dfst, columns = ['ID','HEADLINE']),
               pd.DataFrame(person_dfst, columns = ['PERSON_ID','FIRSTNAME', 'MIDNAME', 'LASTNAME', 'ORGANIZATION', 'ROLE', 'RANK']).drop_duplicates(),
               pd.DataFrame(byline_dfst, columns= ['ID', 'BYLINE']),
               pd.DataFrame(main_dfst, columns= ['ID', 'LEAD','URL','WRD_COUNT','SNPPT','ABST','SOURCE','DATE','DESK','DOC_TYPE','SECTION','SUBSEC','PAGE','TYPE_MTRL'])
               ]   
               
def concat_dfs(list_of_list_of_dfs):
    '''
    loops through list of list of df's.  Each list inside list of lists must consist of 7 
        dataframes. concatenates the df's by index and drops duplicates
        
    params:
    list of list of pandas DatFrames
    
    returns:
    dictionary with 7 keys and pandas DataFrames as values
    
    
    '''
    author_article_concat = pd.DataFrame()
    keyword_concat = pd.DataFrame()
    headline_kick_concat = pd.DataFrame()
    headline_main_concat = pd.DataFrame()
    byline_concat = pd.DataFrame()
    person_concat = pd.DataFrame()
    main_concat = pd.DataFrame()
    
    if not list_of_list_of_dfs[0]:
        print "No tables created - Empty list"
        return {}    
    
    for ldf in list_of_list_of_dfs:
        author_article_concat = pd.concat([author_article_concat, ldf[0]], ignore_index=True).drop_duplicates()       
        keyword_concat = pd.concat([keyword_concat, ldf[1]], ignore_index=True).drop_duplicates()   
        headline_kick_concat = pd.concat([headline_kick_concat, ldf[2]], ignore_index=True).drop_duplicates()   
        headline_main_concat = pd.concat([headline_main_concat, ldf[3]], ignore_index=True).drop_duplicates()   
        byline_concat = pd.concat([byline_concat, ldf[5]], ignore_index=True).drop_duplicates()   
        person_concat = pd.concat([person_concat, ldf[4]], ignore_index=True).drop_duplicates()       
        main_concat = pd.concat([main_concat, ldf[6]], ignore_index=True).drop_duplicates()   
        
    return {'author_article' : author_article_concat,
            'keyword' : keyword_concat,
            'headline_kick' : headline_kick_concat,
            'headline_main' : headline_main_concat,
            'byline' : byline_concat,
            'person' : person_concat,
            'main' : main_concat}   
            
def pysqldf(q):
    '''
    Uses pandasql to give results from passed in query
    params:
    q : string of sql lite query
    
    returns:
    pandas DataFrame
    '''
    return sqldf(q, globals())
        
    
    
    