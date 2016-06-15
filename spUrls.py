from __future__ import print_function
import requests
import sys
import json
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
#from pandasql import *
import scipy.stats as stat
import re
import time
import codecs
import os
import shutil

class nytAPI:

    def __init__(self, search_terms, key): 
        '''
        constructor
        params: search_terms - list of strings
        'f656675cd1a3c5a957395dc6294d86ce%3A9%3A63491393'
        '''
        self.terms = []
        for term in search_terms:
            self.terms.append(term)    
        self.current_time = self.current_milli_time()  
        self.apiky = key
        #self.term = search_term
        
    def __str__(self):
        '''
        print search terms for identifier of object
        '''
        s = ""
        for t in self.terms:
            s = s + " " + t
        return s
    
    def current_milli_time(self):
        return int(round(time.time() * 1000))
    
    #http://developer.nytimes.com/docs/article_search_api  
    #fqu='section_name:("Front Page; U.S.")'
    def getNYTAPIData(self, **kwargs):
        '''
        d['main']['DATE'] = pd.to_datetime(d['main']['DATE'])
        climChange_globWarm_page1_1998_2000_di['main'].index = climChange_globWarm_page1_1998_2000_di['main']['DATE']
        climChange_globWarm_page1_1998_2000_di['main']=climChange_globWarm_page1_1998_2000_di['main'].sort(columns=['DATE'])
        params:
        **kwargs = named arguments that correspond to url keywords
        http://developer.nytimes.com/docs/read/article_search_api_v2
            dates
                begin_date='YYYYMMDD' 
                end_date='YYYYMMDD'
            filter
                fq=['section_name:("Front Page; U.S.")', 
                 'news_desk:("Science Desk" "National Desk")']
                fq=['section_name:("Front Page; U.S.") 
                 AND type_of_material:("Series" "Biography")'] 
        
        returns : dictionary from list of list of df's from list of 7 df's from createDB()     
        '''
        
        lst_of_lst_of_dfs = []
        
        kwdict = kwargs
        if 'begin_date' not in kwdict.keys():                
                kwdict['begin_date'] = '18830101'
        orig_begin_date = kwdict['begin_date']
        ran = False
        #file name
        if not os.path.exists('temp'):
            os.makedirs('temp')
        tm = str(self.current_time)
        output = 'temp/'+tm
        
        for s_term in self.terms: 
            new_term = True
            #create folder            
            
            os.mkdir(output)
            #os.makedirs(temp_dir)
            #if first run delete files    
            if ran == False:
                ran = True
                delFiles = self.get_file_list(output)
                for f in delFiles:
                    os.remove(output+'/'+f)   
            
            
            #file to write to
            fl = codecs.open(output+'/'+tm+'.txt', "w", encoding='utf-8')     
            
            #articles from query
            hits = 0  
            #page of results - groups of ten
            offSet = 0  
            #if empty set returned
            no_docs = False
            #facet fields - pretty prints some json with summary sums            
                    
            
                
            last_date = orig_begin_date
            
            while hits >= 0:
                #the API retrieves pages of results, 10 per page, gets 1 page per url call 
                #param builder
                
                if new_term == True:
                    kwdict['begin_date'] = orig_begin_date
                
                #nyt api allows up to page 100 for each call
                #change begin dates after that, restart offSet number
                if offSet > 100:
                    kwdict['begin_date']=last_date
                    offSet = 0
                
                #request url builder
                rq = requests.get(self.urlBuilder(s_term, offSet, kwdict))                
                
                #text from url call                                     
                rqText = rq.text                
                #json parser
                try:
                    getHits = json.loads(rqText)  
                except:
                    getHits = {}
                    print(rqText)
                    
                if new_term == False:    
                    try:
                        last_date = re.sub(r'[-]', '', getHits['response']['docs'][-1]['pub_date'][0:10])
                    except:
                        last_date = ''
                #print('lastDate '+last_date)
                if offSet == 0:
                    if 'response' in getHits.keys():
                        hits = getHits['response']['meta']['hits']
                        print(str(s_term) + " - HITS: " + str(hits)) 
                        print('\n')
                        if hits == 0:                    
                            no_docs = True
                    else:
                        print( "Bad File - " + rq.text)
                        print('\n')
                        no_docs = True  
                #subtract ten hits, will run until hits goes negative
                hits -= 10    
                #print json to file
                print(rqText,file=fl)                
                #some helpful console values
                if offSet == 0:
                    print(rq.url)
                    print('\n')
                    
                if offSet % 10 == 0:
                    print("page: " + str(offSet))  
                    print('lastDate '+last_date)
                    
                offSet += 1  
                new_term = False
               
            fl.close()  
            
            editFiles = self.get_file_list(output)
            #create txt file
            #with codecs.open('final.txt', 'w', encoding='utf-8') as outfile:
                #for fname in editFiles:
                    #with codecs.open('temp/'+fname, encoding='utf-8') as infile:
                        #for line in infile:
                            #outfile.write(line)   
            #build list of lists 
            
            
            
            if no_docs == False:
                lst_of_lst_of_dfs.append(self.createDB(output+'/'+editFiles[0]))        
        if os.path.exists('temp'):
            shutil.rmtree('temp')
        
        #concat list of lists into a dictionary   
        if len(lst_of_lst_of_dfs) > 0:
            return self.concat_dfs(lst_of_lst_of_dfs) 
            
    def urlBuilder(self, q, offset, kwargsDict):  
        '''
        params:
        q = search term
        offset = page of results
        kwagsDict = keyword arguments for url
            dates
                begin_date='YYYYMMDD' 
                end_date='YYYYMMDD'
            filter
                fq=['section_name:("Front Page; U.S.")', 
                 'news_desk:("Science Desk" "National Desk")']
                fq=['section_name:("Front Page; U.S.") 
                 AND type_of_material:("Series" "Biography")']
                
        returns: string url
        '''
        #base uri
        baseUrl = 'http://api.nytimes.com/svc/search/v2/articlesearch.json?'
        #api key
        keycc = 'api-key=' + self.apiky
        #facets
        fcts = '&facet_filter=true&facet_field=source,section_name,subsection_name,document_type,type_of_material,day_of_week,pub_year,pub_month'
        #buikding uri
        baseUrlKeyFacetsPageQuery = baseUrl+keycc+fcts+'&page='+str(offset)+'&q='+q
                
        for key, value in kwargsDict.items():
            #list of filtered query
            if str(key) == 'fq':
                for f in value:
                    baseUrlKeyFacetsPageQuery += '&fq=' + f
            #start date
            if str(key) == 'begin_date':
                baseUrlKeyFacetsPageQuery += '&begin_date=' + value
            #end date
            if str(key) == 'end_date':
                baseUrlKeyFacetsPageQuery += '&end_date=' + value
            #sort - default sorts by relevance
            baseUrlKeyFacetsPageQuery += '&sort=oldest'
            
        
        return baseUrlKeyFacetsPageQuery       
    
    def get_file_list(self, tf):
        '''
        for scalbility in case multiple files are needed in temp folder in future
        '''
        dirListing = os.listdir(os.path.realpath(tf))
        editFiles = []
        for item in dirListing:
            if ".txt" in item:
                editFiles.append(item) 
        return editFiles
        
    
    
    def createDB(self, inputFile):
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
        
        pagescct = codecs.open(inputFile,  encoding='utf-8')
        for line2 in pagescct:
            try:            
                pagescc2 = json.loads(line2)
            except ValueError:
                print(pagescc2+" Something didn't load 'pagescc2 = json.loads(line2)'")
                print('\n')
            #pagescc2 = json.loads(line2)
            if 'response' not in pagescc2.keys():            
                print( "EOF")
                print('\n')
                break
                
            if len(pagescc2['response']['docs']):
                for key in pagescc2['response']['docs']:  
                    #print "----KEYWORD TABLE----"
                    if 'keywords' in key.keys() and key['keywords']:
                        for word in key['keywords']:                
                            keyword_dflst.append(list( [key['_id'], word['value']] )) 
                    else:
                        keyword_dflst.append(list( ['','']))
                        
                    #print "----HEADLINE MAIN TABLE----"
                    hline = ''
                    if 'main' in key['headline'].keys() and key['headline']['main']:
                        hline = key['headline']['main']
                        
                    headline_main_dfst.append(list([key['_id'], hline]))
                    
                    #print "----HEADLINE KICKER TABLE----"
                    if 'kicker' in key['headline'].keys():               
                        headline_kicker_dfst.append(list([key['_id'], key['headline']['kicker']]))  
                    else:
                        headline_kicker_dfst.append(list(['','']))
                    
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
                                    else:
                                     person_article.append(list(['','']))
                                     person_dfst.append(list(['','','','','','','']))
                    #print "----BYLINE TABLE----" 
                    if 'byline' in key.keys() and key['byline'] != None:
                        if 'original' in key['byline'] and key['byline']['original'] != '':
                            byline_dfst.append( list([key['_id'], key['byline']['original']]) ) 
                        else:
                            byline_dfst.append(list([key['_id'], '']))            
                    
                    #print "----MAIN TABLE----" 
                    main_dfst.append( list([key['_id'],
                                    hline,
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
         
            #if len(pagescc2['response']):
                #if len(pagescc2['response']['facets']):  
                    #print(json.dumps(pagescc2['response']['facets'], sort_keys=True,
                                 #indent=4, separators=(',', ': ')))     
            #print('\nFACETS:')
            #for day in pagescc2['response']['facets']['day_of_week']['terms']:
                #print(day['term']+' = '+str(day['count']))
            
        #for facet in pagescc2['response']['facets'].items():
            #print(str(facet[0])) 
            #for term in facet[1]['terms']:
                #print(' '+str(term['term'])+' = '+str(term['count']))
            #print('\n')     
        
        list_of_dfs = [pd.DataFrame(person_article, columns=['PERSON_ID','ID']),
                   pd.DataFrame(keyword_dflst, columns = ['ID','KEYWORD']),
                   pd.DataFrame(headline_kicker_dfst, columns = ['ID','KICKER']),
                   pd.DataFrame(headline_main_dfst, columns = ['ID','HEADLINE']),
                   pd.DataFrame(person_dfst, columns = ['PERSON_ID','FIRSTNAME', 'MIDNAME', 'LASTNAME', 'ORGANIZATION', 'ROLE', 'RANK']).drop_duplicates(),
                   pd.DataFrame(byline_dfst, columns= ['ID', 'BYLINE']),
                   pd.DataFrame(main_dfst, columns= ['ID', 'HEADLINE', 'LEAD','URL','WRD_COUNT','SNPPT','ABST','SOURCE','DATE','DESK','DOC_TYPE','SECTION','SUBSEC','PAGE','TYPE_MTRL'])
                   ] 
                
        return list_of_dfs
                   
    def concat_dfs(self, list_of_list_of_dfs):
        #print(str(len(list_of_list_of_dfs))+" dfs")
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
        
        if not list_of_list_of_dfs:
            print ("No tables created - Empty list")
            return {}    
        #concatentate and drop duplicates
        for ldf in list_of_list_of_dfs:
            author_article_concat = pd.concat([author_article_concat, ldf[0]], ignore_index=True).drop_duplicates()
            keyword_concat = pd.concat([keyword_concat, ldf[1]], ignore_index=True).drop_duplicates() 
            headline_kick_concat = pd.concat([headline_kick_concat, ldf[2]], ignore_index=True).drop_duplicates()
            headline_main_concat = pd.concat([headline_main_concat, ldf[3]], ignore_index=True).drop_duplicates()
            byline_concat = pd.concat([byline_concat, ldf[5]], ignore_index=True).drop_duplicates()
            person_concat = pd.concat([person_concat, ldf[4]], ignore_index=True).drop_duplicates() 
            main_concat = pd.concat([main_concat, ldf[6]], ignore_index=True).drop_duplicates()
        
                
        #reindex dateframes
        main_concat.index=range(0,len(main_concat))
        author_article_concat.index=range(0,len(author_article_concat))
        keyword_concat.index=range(0,len(keyword_concat))
        headline_kick_concat.index=range(0,len(headline_kick_concat))
        headline_main_concat.index=range(0,len(headline_main_concat))
        byline_concat.index=range(0,len(byline_concat))
        person_concat.index=range(0,len(person_concat))
        
            
        return {'author_article' : author_article_concat,
                'keyword' : keyword_concat,
                'headline_kick' : headline_kick_concat,
                'headline_main' : headline_main_concat,
                'byline' : byline_concat,
                'person' : person_concat,
                'main' : main_concat}   
                
    '''
    def pysqldf(q):
       
        Uses pandasql to give results from passed in query
        params:
        q : string of sql lite query
        
        returns:
        pandas DataFrame
       
        return sqldf(q, globals())
    '''   
    
    def date_calc(self, main_table):
       '''
       params:
       main_table : table with all docs returned.  'main' in returned dictionary from concat_dfs
        
       returns : float percent of days with article
       '''
       main_table['DATE'] = pd.to_datetime(main_table['DATE'])
       #main_table['WRD_COUNT'] = main_table['WRD_COUNT'].astype('int')
       #words = np.sum(main_table['WRD_COUNT'])
       #avg = (words * 1.0) / len(main_table)
       #print('Average wrd count ' + str(avg))    
       main_table=main_table.sort('DATE')
       main_table.index=range(0,len(main_table))
       dateRange = main_table['DATE'][main_table.index[-1]] - main_table['DATE'][0]
       rows = len(main_table.index)  
       weeks = dateRange.days/7.0
       months = dateRange.days/30.4
       years = dateRange.days/364.25
       print (str(rows) + ' articles')
       print(str(dateRange.days) + ' days')
       print(str(weeks) + ' weeks')
       print(str(months) + ' months')
       print(str(years) + ' years')
       return (float(rows) / dateRange.days) * 100
