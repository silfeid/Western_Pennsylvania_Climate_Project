# -*- coding: utf-8 -*-
"""
Created on Fri December 10 20:55:41 2021

@author: Adam Brode (brodeam@gmail.com) 

"""
#os.path will be used to pull files from a specified directory according to certain criteria, and to create directories for storing saved plots based on the names of the dataframes plotted.
import os.path
#Pandas is used to create the dataframes from .csv files, to generally  manipulate the data, create new dataframes from the starting set, and to plot (via matplotlib)
import pandas as pd
#Matplotlib.pyplot is used for plotting, in some instances directly, in others Pandas 'piggybacks' on it in order to plot a dataframe.
import matplotlib.pyplot as plt
#Sys is used only for the func_quit() function, which does just what you'd think it does.  It quits.  Some say it's the best at quitting.
import sys
#This is just to allow me to adjust the plotting parameters to autolayout, below.
from matplotlib import rcParams
#datetime alows strings ina  given format to be interpreted as datetime (or here, just date) objects; although pandas has a similar function built-in, here I wanted to compare the starting and ending dates by which the user can slice the dataframes, in order to make sure that the specified end date does not precede the specified start date.  In hindsight, I could probably also have used it to control the input for those dates, instead of the exception handling function for integers that I built on, but oh well.  If it ain't broke...
from datetime import datetime as dt
#Add a real comment here
import numpy as np
#Add another real comment here
from git import Repo

#The auto-layout feature should prevent some text etc. from being cut-off; I figured better safe than sorry, and it seems to at least not have caused any problems.
rcParams.update({'figure.autolayout': True})

#The very simplest function, which probably doesn't need to exist, as just sys.exit() would get the job done.
def func_quit():
    print('\nThanks for using the Western PA Winter Weather Plotter - Have a nice day!')
    sys.exit()

def download_files():
    
    if os.path.isdir('Western_PA_Climate_Project/Climate_Data') is False:
        Repo.clone_from('https://github.com/silfeid/Western-PA-Climate-Database.git', 'Western_PA_Climate_Project/Climate_Data')
    
    else:
        pass

def move_stations():
    
    if os.path.isfile('Western_PA_Climate_Project/Climate_Data/Stations.csv') is True:
        os.mkdir('Western_PA_Climate_Project/Climate_Data/Stations')
        os.replace('Western_PA_Climate_Project/Climate_Data/Stations.csv', 'Western_PA_Climate_Project/Climate_Data/Stations/Stations.csv')
        
    else:
        pass
    
def set_initial_dates():

    if os.path.isfile('Western_PA_Climate_Project/Misc/ending_date.txt') is False:
        with open('Western_PA_Climate_Project/Misc/ending_date.txt', 'w') as end_date:
            end_date.write('12/31/2013')
            end_date.close()
        
    if os.path.isfile('Western_PA_Climate_Project/Misc/starting_date.txt') is False:
        with open('Western_PA_Climate_Project/Misc/starting_date.txt', 'w') as start_date:
            start_date.write('1/1/1970')
            start_date.close()
    
def make_project_directories():
    
        if os.path.isdir('Western_PA_Climate_Project') is False:
            os.mkdir('Western_PA_Climate_Project')

        if os.path.isdir('Western_PA_Climate_Project/Misc') is False:
            os.mkdir('Western_PA_Climate_Project/Misc')

        if os.path.isdir('Western_PA_Climate_Project/Plots') is False:
            os.mkdir('Western_PA_Climate_Project/Plots')

        if os.path.isdir('Western_PA_Climate_Project/Plots/Single_Stations') is False:
            os.mkdir('Western_PA_Climate_Project/Plots/Single_Stations')

        if os.path.isdir('Western_PA_Climate_Project/Plots/All_Stations') is False:
            os.mkdir('Western_PA_Climate_Project/Plots/All_Stations')
            
        if os.path.isdir('Western_PA_Climate_Project/Plots/All_Stations/Means_Comparisons') is False:
            os.mkdir('Western_PA_Climate_Project/Plots/All_Stations/Means_Comparisons')            

        if os.path.isdir('Western_PA_Climate_Project/Plots/Correlation_Plots') is False:
            os.mkdir('Western_PA_Climate_Project/Plots/Correlation_Plots')            

        if os.path.isdir('Western_PA_Climate_Project/Summary_Stats') is False:
            os.mkdir('Western_PA_Climate_Project/Summary_Stats')

def build_dfs():
    
    #The individual station .csv files, downloaded from https://www.ncdc.noaa.gov/, were manipulated in MS SQL Server, where they constitute a relational database, and then saved in their final form in the directory below.
    directory = 'Western_PA_Climate_Project/Climate_Data/'
    
    #An empty dictionary, to be populated with station names as keys and dataframes as values.
    df_dict = {}
    
    #this for loop iterates over all of the files in the directory, and if they end in .csv (they all should, but one never knows), then it uses the read_csv function built into Pandas to turn them into dataframes, using the filename as the name of the dataframe - the keys in the dictionary referenced above (df_dict).
    for filename in os.listdir(directory):
    
        if filename.endswith('.csv'):
            #Get rid of the .csv so we can have the station name alone as the key 
            filename = filename.replace('.csv', '')
            #Add .csv back to the path so that it'll read properly
            df_dict[filename] = pd.read_csv(directory+filename+'.csv')
            #Set the ObDate Column (Observation Date) as the index; these are also the primary keys for the tables in the SQL Database; StationCode is the foreign key that relates to the Stations table, containing geographic information on each station.
            df_dict[filename] = df_dict[filename].set_index('ObDate')
    
    #The next bit of code uses the os module to create new directories at a specified location (within parent_dir), one for each station location identified above.        
    parent_dir = "Western_PA_Climate_Project/Plots/Single_Stations/"
    
    #Each key represents a station, so we just iterate over the keys in this for loop.
    for key in list(df_dict.keys()):
        
        #the filepath for the new directory = the parentdir + the key name
        path = os.path.join(parent_dir, key)
        #Check to make sure that directory doesn't already exit; if it does, we simply "pass" ;this way new stations can be added to the database/program without creating a headache.
        if os.path.isdir(path) is False:
            #os.mkdir creates a directory at the specified filepath
            os.mkdir(path)
        else:
            pass
    #Return the df_dict, which we'll use throughout the rest of the program
    return df_dict

#The integer checker just checks input from the user and as you've already guessed, makes sure that it's an integer; if it's not, a message is displayed and the user is prompted once more. At this stage, any integer will do.
def integer_checker(slicer_datum):
  while True:
    try:
       user_input = int(input(slicer_datum))       
    except ValueError:
       print("\nInput must be an integer. Try again.")
       continue
    else:
       return user_input 
       break 
#Here, we'll use the integer checker to solicit a start date from the user.  Once we get an integer, we can perform the necessary checks to make sure that it's a valid month, valid day for that month, and valid year.
def get_start_date():
    
    print('Pick the date range for which you wish to plot data.  The first available date is January 1, 1970; the last available date is December 31, 2013.')
    #Run integer checker to get our user input.
    start_month = integer_checker('Select starting month: ')
    #Should be pretty clear; needs to be an integer that corresponds to a month.
    if start_month > 0 and start_month < 13:
        pass
    else:
        print('\nMust be integer 1-12. Try again.')
        start_month = integer_checker('Select starting month: ')
    
    #Now that the necessary operations have been performed, we convert the start_month to a string so that it can be concatenated and written to a .txt file as needed.
    start_month = str(start_month)
    
    #Pretty similar operations are performed on the start_day variable, as below, mutatis mutandis etc. etc.
    start_day = integer_checker('Select starting day: ')

    thirtiers = ['9', '4', '6', '11']
    
    if start_month in thirtiers:
        while start_day > 30 and start_day <32:
            print('\nDay does not match to month')
            start_day = integer_checker('Select starting day: ')
            
    if start_month == '2':
        print(start_month)
        while start_day > 28:
            print('\nDay does not match to month; leap days are excluded from this program.')
            start_day = integer_checker('Select starting day: ')
               
    else:
        while start_day > 31 or start_day < 0:
            print('\nNo month has that number of days in it.')
            start_day = integer_checker('Select starting day: ')

    start_day = str(start_day)
    
    #And grab the start year; here, we just make sure it's not before 1970 or after 2013.        
    start_year = integer_checker('Select starting year: ')
    
    if start_year < 1970 or start_year > 2013:
        print('\nYear must be between 1970 and 2013, inclusive. Try again.')
        start_year = integer_checker('Select starting year: ')
    
    start_year = str(start_year)
    
    #We concatenate the start month, day, and year to get our start_date, adding the slashes as needed...
    start_date = start_month+'/'+start_day+'/'+start_year
    #And write that date to a .txt file, so that it can be summoned by the program even after restarting (the last used date, i.e. the date last written to the .txt file, is the default start date used by the program; same goes for the end date, below.)
    start_file = open('Western_PA_Climate_Project/Misc/starting_date.txt', 'w')
    start_file.write(start_date)
    start_file.close()
    
    print('\nStarting date successfully chosen.  Next, pick an ending date.')

#This function works almost identically to the get_start_date() function above, so only those aspects which differ between the two are commented on below.
def get_end_date():
    
    #Read the start_date in from the .txt file, since we need to compare it to the end date to make sure that the latter doesn't come before the former, chronologically
    start_file = open('Western_PA_Climate_Project/Misc/starting_date.txt', 'r')
    start_date = start_file.read()
    #And here we use the datetime module and its strptime function to parse a string as a datetime object (here, just a date, as specified by the .date() tag at the end the function).
    check_start_date = dt.strptime(start_date, '%m/%d/%Y').date()
    
    end_month = integer_checker('Select ending month: ')
    if end_month > 0 and end_month < 13:
        pass
    else:
        print('\nMust be integer 1-12. Try again.')
        end_month = integer_checker('Select ending month: ')
    
    end_month = str(end_month)

    end_day = integer_checker('Select ending day: ')

    thirtiers = ['9', '4', '6', '11']
    
    if end_month in thirtiers:
        while end_day > 30 and end_day <32:
            print('\nDay does not match to month')
            end_day = integer_checker('Select ending day: ')
            
    if end_month == '2':
        print(end_month)
        while end_day > 28:
            print('\nDay does not match to month; leap days are excluded from this program.')
            end_day = integer_checker('Select ending day: ')
               
    else:
        while end_day > 31 or end_day < 0:
            print('\nNo month has that number of days in it.')
            end_day = integer_checker('Select ending day: ')

    end_day = str(end_day)
            
    end_year = integer_checker('Select ending year: ')
    
    if end_year < 1970 or end_year > 2013:
        print('\nYear must be between 1970 and 2013, inclusive. Try again.')
        end_year = integer_checker('Select ending year: ')
        
    end_year = str(end_year)
    
    end_date = end_month+'/'+end_day+'/'+end_year
    #Here we turn the end_date into a datetime object as well...
    check_end_date = dt.strptime(end_date, '%m/%d/%Y').date()
    #...And use a simple comparison operator to determine that it does indeed come after the start date and not before.  If it didn't, we'd just start the get_end_date function over after displaying a message telling the user what they did wrong.
    if check_end_date < check_start_date:
        print('\nEnd date must come after start date!')
        get_end_date()
    
    else:
        
        end_file = open('Western_PA_Climate_Project/Misc/ending_date.txt', 'w')
        end_file.write(end_date)
        end_file.close()
        
        print('\nEnding date successfully chosen.')

#This function retrieves the starting and ending dates saved to .txt via their respective functions.  Using this method allows us to simply retrieve the last dates used if the user doesn't wish to enter new ones, giving a default set of dates.    The code here should be self-explanatory.
def build_slicer_date():
    
   start_file = open('Western_PA_Climate_Project/Misc/starting_date.txt', 'r')
   start_date = start_file.read()
   
   end_file = open('Western_PA_Climate_Project/Misc/ending_date.txt', 'r')
   end_date = end_file.read()
   
   return start_date, end_date
#This wee function simply prints the current start and end dates selected in case the user has forgotten them or is using the program for the first time (for them).
def show_slicer_dates():
    start_date, end_date = build_slicer_date()
    print('\nStart date: '+start_date)
    print('End date: '+end_date)

#Having built our dataframes, we now use the start and end dates arrived at above to slice each dataframe by index (which you'll remember we set to the 'ObDate' column earlier), so that each dataframe is truncated to include only those dates (ObDates) which fall in the range specified by the user.   
def slice_dfs():
    #Retrieve the dataframes
    df_dict = build_dfs()
    #Retrieve the start and end dates.
    start, end = build_slicer_date()
    #Up to this point, the ObDate column (our index for each dataframe) has been in the format of a string; this step converts it to datetime object, something that's thankfully built into Pandas and which Pandas can readily interpret.  Note that although it retains its former name when the dataframe is displayed, the ObDate column can only be summoned as df.index once it's been set as such, and can't be called using its former column name (ObDate).
    for df in df_dict.values():
        df.index = pd.to_datetime(df.index)
    #Thankfully Pandas is smart, and although 'start' and 'end' are just strings formatted in the same way as our datetime objects in ObDate/index, Pandas recognizes them as such when we use them to slice the index; the for loop below does this for each dataframe, summoning it from the dictionary using the name of its key and then slicing use the .loc function of Pandas.    
    for key in df_dict.keys():
        df_dict[key] = df_dict[key].loc[start:end]
    #We will want the start and end dates later, not to manipulate the df's (that step's done with), but to use as variables in displaying plots and in creating unique filenames for saving.  Since we're not working with any unsliced df's (the user simply picks the first and last available dates if they wish to view data for the entire date range), we'll just replace the old df_dict with this one.
    return df_dict, start, end

#This function plots one of the four variables charted in each dataframe, all of them daily: Snowfall, Snow Depth, Minimum Temperature and maximum Temperature, for a single station.  The user will first pick which station they want to view, then which variable.
def single_df_plotter():
    
    #Grab the df's and start, end dates from the slice_dfs() function
    df_dict, start, end = slice_dfs()
    
    #I used a dictionary of the station names to control user input; they'll just pick a number from the list printed below and the dictionary is then used to transform that into the actual station name/dictionary key needed.
    station_choices = {1:'Butler', 2:'Clarion', 3:'Confluence', 4:'Dubois', 5:'Erie', 6:'Franklin', 7:'Indiana', 8:'Laurel_Mountain', 9:'New_Castle', 10:'Pittsburgh', 11:'Tionesta', 12:'Uniontown', 13:'Warren', 14:'Waynesburg'}
    
    print('Pick a station for which to plot data:\n\n1.Butler\n2.Clarion\n3.Confluence\n4.Dubois\n5.Erie\n6.Franklin\n7.Indiana\n8.Laurel Mountain\n9.New Castle\n10.Pittsburgh\n11.Tionesta\n12.Uniontown\n13.Warren\n14.Waynesburg')
    #The integer checker from above is used to validate user input here as well.
    df_name_choice = integer_checker('\nStation Choice: ')
                                
    #If they picked an integer that corresponds to a station, we summon the value associated with that integer key here.
    if df_name_choice in station_choices:
        df_name_chosen = station_choices[df_name_choice]
    #If they entered an invalid integer, then we return them to the main menu after telling them what they did wrong.  Restarting the function will produce errors which I don't quite grok as yet.
    else:
        print('Not a valid station number. Try again.')
        menu()
    
    #Summon the dataframe we want from df_dict as "df_choice"
    df_choice = df_dict[df_name_chosen]

    #This probably can be deleted, but just a print statement to confirm to the user which station they've chosen.
    print('\nStation: '+df_name_chosen)

    #A dictionary of the variables for plotting, on the model of the one above for stations.
    plot_var_dict = {1:'Snowfall', 2:'Snowdepth',3:'TempMin', 4:'TempMax'} 
    
    #Versions of the same variables for 'pretty printing' (display on the plots, saving, etc.)
    print_var_dict = {1:'Daily Snowfall', 2:'Daily Snow Depth',3:'Minimum Daily Temperature',4:'Maximum Daily Temperature'}
    
    plot_var_choice = integer_checker('Pick a variable for plotting: 1.Daily Snowfall 2. Daily Snow Depth 3. Minimum Daily Temperature 4. Maximum Daily Temperature Choice: ')
    
    #Same mechanism as above for input validation: boot 'em to the main menu if the integer isn't appropriate...I'm 100% sure there's a better way to do this, but as I mentioned above, restarting the function itself produces unexpected errors, and I'm pretty much out of time at this point...perfect enemy of good mumble mumble...

    if plot_var_choice in plot_var_dict:
        pass
        
    else:
        print('Choice not valid.  Returning to main menu.')
        menu()
    
    #We'll turn that int to a string and use it grab the correct print variable from the print variable dictionary we created a few lines above.  Just so it'll look all nice later on.

    print_variable = print_var_dict[plot_var_choice]
    
    #Picking what units to display on the y axis of the plot (below); since choices one and two for plot_var are measured in inches, and 3 and 4 are measured in degrees Fahrenheit, the if clause here is pretty simple.
    if int(plot_var_choice) < 3:
        print_unit = ' (in)'
    else:
        print_unit = ' (°F)'
    
    #Use our plot variable choice to grab the right column name from the plot var dictionary we created at the start of the function code.
    plot_variable = plot_var_dict[plot_var_choice]

    #We use the plot variable uh...variable (a string) to select the appropriate column from our chosen dataframe and plot it using Pandas; if we ask Pandas to plot a single column, i.e., a Series, it will automatically use the index as the x-axis and the values in the series as the y-axis, handily setting the ticks and scale all by itself.  Below I've used the variables that I created above to assign the plot a title containing relevant information and labels for the x and y axes.  Ms stands for marker size, and figsize is x*y dimensions, I believe in inches.  Seems that way anyway.  Certainly isn't cm.  Maybe hundreds of pixels?  I dunno.
    var_stat_plot = df_choice[plot_variable].plot(title=df_name_chosen+' '+print_variable+' '+start+'-'+end, xlabel='Year', ylabel=print_variable+print_unit, style='.', ms=6, figsize = (8, 4))
    
    #The date variables have to be modified in order to be used in the filename (we're going to save it to the directory corresponding to the station name that we created way back at the start) because Windows quite sensibly won't allow filenames with /.  We could have avoided the use of slashes in the dates entirely, of course, but the .csv files came in that format, and frankly that's just how Americans write the date - I prefer '24.XII.2021' for Christmas Eve, e.g., myself, but I'm just a voice in the wilderness...
    start = start.replace('/', '.')
    end = end.replace('/', '.')

    #Retrieve the figure we created above in order to save it; I frankly don't know why it has to be done this way, but I believe that it does, at least if you don't want the figure to be displayed first, which I don't - I find that the display area in the console is too small for convenient viewing, and that the display of the plots clutters the display and distracts the user from the efficient retrieval of the plots.  My program saves all the plots to directories created for the purpose, to be viewed and compared later.  They're intuitively named, and the program informs the user of the save locations upon successful export as well.
    fig = var_stat_plot.get_figure()
    #The filepath for saving uses the df_name_chosen variable to get into the final directory, then creates a unique name for the plot using the station name, variable plotted, and date range used.  If someone entered the same variables (station, plot variable, and dates), the plot would be overwritten, but only with the exact same plot, so, no harm done.
    fig.savefig('Western_PA_Climate_Project/Plots/Single_Stations/'+df_name_chosen+'/'+df_name_chosen+'_'+print_variable+'_'+start+'-'+end+'.jpg')
    #Closing the plot for prudence's sake, and RAM too, I suppose.
    plt.close(fig)
    print('\nPlot successfully exported to \'Plots/Single_Stations/'+df_name_chosen+'\'')   

#This function graphs the mean for each weather variable for each station for the specified date range, and also averages the average for all stations, graphing that as well.
def descriptive_stats_grapher():
    
    #I'm going to stop describing code that's already been addressed from here on out.
    df_dict, start, end = slice_dfs()
    
    #turn the keys of the df_dict into a list that we can iterate over; don't necessarily have to cast it as a list, but seems a bit safer...
    df_keys =list(df_dict.keys())
    
    #This iterates over the whole dictionary of dataframes and replaces each with the (vastly smaller) dataframe that one gets from calling the df.describe() function; this returns means, standard deviations, counts, and the like.
    for key in df_keys:
        df_dict[key] = df_dict[key].describe()

    #For now, we don't care about anything in the described dfs other than the means for each weather variable, so we're going to redefine each value in the df_dict(ionary) as the Pandas Series we get when we use .loc['mean']; just to reiterate, what we now have in df_dict is a dictionary in which the keys are the station names (14 as of the time of writing) and the values are now just a series of 4 numbers, each the mean of a different weather variable (Snowfall, Snowdepth, TempMin and TempMax, in that order).
    for key in df_keys:
        df_dict[key] = pd.Series(df_dict[key].loc['mean'])
    
    #Drop null values from the series in our dictionary    
    for key in df_keys:
        df_dict[key] = df_dict[key].dropna()
        #If dropping nulls results in an empty series, then we get rid of that key/value pair entirely; not doing so would create problems later, especially in our calculation of the average of all stations.
        if df_dict[key].empty is True:
            del df_dict[key]
    
    #Same exact code as above, but we have to run it again to exclude the entries that may have been deleted when we dropped all our empty series.    
    df_keys = list(df_dict.keys())
        
    #Now we make a new set of four dictionaries, one for each weather variable; each will be loaded up with one key/value pair for each station left in df_keys, the key being the station name and the value just being a number, whatever the mean for that weather variable at that station was.
    snowfall_dict = {}
    snowdepth_dict = {}
    tempmin_dict = {}
    tempmax_dict = {}
    
    #Load them dictionaries up!  We cast them to float here to make sure that our calculations of the average of all stations won't go awry (see below)
    for key in df_keys:
        if key in df_dict:
            snowfall_dict[key] = float(df_dict[key].loc['Snowfall'])
            snowdepth_dict[key] = float(df_dict[key].loc['Snowdepth'])
            tempmin_dict[key] = float(df_dict[key].loc['TempMin'])      
            tempmax_dict[key] = float(df_dict[key].loc['TempMax'])  
    
    #Now that we've built our dictionaries, we can let the user know what's going on via the print statement below, and solicit their input as to what they'd like to do.
    av_var_choice = input('Pick a variable and to see the graph of its average for each station.  Note that some stations\' reporting period is longer or shorter than others, and each station\'s records may have gaps, so a comparison of means may be unequal. If a station does not appear in the plot, that means that there was no data for that station during the specified time frame.\n\n1. Average Daily Snowfall\n2. Average Daily Snow Depth\n3. Average Daily Minimum Temperature\n4. Average Daily Maximum Temperature\n\nChoice: ')
    
    #A list of what you can do.  I have to admit that I don't even know why I did this the way that I did; it's resulted in much more code than I needed to achieve my ends, albeit mostly just copied and pasted with the variables replaced for each dictionary.  Kind of ugly, although it works.  At any rate, I'm only going to comment on the first block of code here, since the others are identical other than the differing variable names.
    valid_av_var_choices = ['1', '2', '3', '4']
    
    if av_var_choice in valid_av_var_choices:
        
        if av_var_choice == '1':
            #We're actually just going to grab the numbers and names we need for our bar graph and put them into list form, as the simplest way to go about things and most foolproof.  So we make two empty lists to populate with some for loops.
            snowfall_values = []
            snowfall_keys = []
            #The floats were pretty big, so we'll round them, then append them to the values list we made just above.
            for value in snowfall_dict.values():
                value = round(value, 1)
                snowfall_values.append(value)
            
            #Calculate the average of all stations...    
            mean_snowfall = (sum(snowfall_values))/(len(snowfall_values))
            #I believe that it wouldn't let me do this in one line, for whatever reason.
            mean_snowfall = round(mean_snowfall, 1)
            #Stick the average of all stations onto the list as the final value
            snowfall_values.append(mean_snowfall)
             
            #Populate the keys list for the x-axis
            for key in snowfall_dict.keys():
                snowfall_keys.append(key)                 
            #And we'll add in the All Stations Average element to correspond to the average that we stuck into the values list above.    
            snowfall_keys.append('All Stations Average')   
            
            #And now we build our bar graph, listing x, then y, and picking a color.                  
            plt.bar(snowfall_keys, snowfall_values, color = 'skyblue')
            #Just tweaking the xticks.
            plt.xticks(rotation =60, fontsize = 10, ha = 'right')
            #We'll give it a title...
            plt.title('Average Snowfall, All Stations '+start+'-'+end)
            #Define the margins and set the layout to tight to keep things neat
            plt.margins(0.1)
            plt.tight_layout()
            
            #This for loop displays the value of each bar above it; optional, but a nice touch, I think.
            for item in range(len(snowfall_values)):
                plt.text(item, snowfall_values[item], snowfall_values[item], ha='center', rotation=0, verticalalignment='center_baseline')
                
            #Set our labels for the x and y axes; pretty straightforward.    
            plt.xlabel('Station', fontweight='bold', color = 'black', fontsize='12')
            plt.ylabel('Snowfall (inches)', fontweight='bold', color = 'black', fontsize='12')            
            
            #Here again, we fix our start and end dates so that they can be used in the ultimate filepath of the plot.
            start = start.replace('/', '.')
            end = end.replace('/', '.')            
            
            #And then we save the plot...
            plt.savefig('Western_PA_Climate_Project/Plots/All_Stations/Means_Comparisons/Snowfall_All_Stations-'+start+'-'+end+'.jpg')
            #And tell our user that we've done so...
            print('\nFigure saved to directory \'Plots/All_Stations/Means_Comparisons\'')
            #And close the plot.
            plt.close()
        
        #The restof these are essentially identical to "if av_var_choice == '1'", so I'll skip over them in some embarrassment at the inefficiency of my coding for this function...
        if av_var_choice == '2':
          
            snowdepth_values = []
            snowdepth_keys = []
            for value in snowdepth_dict.values():
                value = round(value, 1)
                snowdepth_values.append(value)
                
            mean_snowdepth = (sum(snowdepth_values))/(len(snowdepth_values))
            mean_snowdepth = round(mean_snowdepth, 1)
            snowdepth_values.append(mean_snowdepth)    
                
            for key in snowdepth_dict.keys():
                snowdepth_keys.append(key)       
                
            snowdepth_keys.append('All Stations Average')
        
            plt.bar(snowdepth_keys, snowdepth_values, color = 'thistle')
            plt.xticks(rotation =60, fontsize = 10, ha = 'right')
            plt.title('Average Snow Depth, All Stations '+start+'-'+end)
            plt.margins(0.1)
            plt.tight_layout()
            for item in range(len(snowdepth_values)):
                plt.text(item, snowdepth_values[item], snowdepth_values[item], ha='center', rotation=0, verticalalignment='center_baseline')
            plt.xlabel('Station', fontweight='bold', color = 'black', fontsize='12')
            plt.ylabel('Snow Depth (inches)', fontweight='bold', color = 'black', fontsize='12')        
            
            start = start.replace('/', '.')
            end = end.replace('/', '.')            
            
            plt.savefig('Western_PA_Climate_Project/Plots/All_Stations/Means_Comparisons/Snow_Depth_All_Stations-'+start+'-'+end+'.jpg')
            print('\nFigure saved to directory \'Plots/All_Stations/Means_Comparisons\'')
            plt.close()
            
        if av_var_choice == '3':
          
            tempmin_values = []
            tempmin_keys = []
            
            for value in tempmin_dict.values():
                value = round(value, 1)
                tempmin_values.append(value)
                
            mean_tempmin = (sum(tempmin_values))/(len(tempmin_values))
            mean_tempmin = round(mean_tempmin, 1)
            tempmin_values.append(mean_tempmin)     
                
            for key in tempmin_dict.keys():
                tempmin_keys.append(key)         
                
            tempmin_keys.append('All Stations Average')    
                
            plt.bar(tempmin_keys, tempmin_values, color = 'cadetblue')
            plt.xticks(rotation =60, fontsize = 10, ha = 'right')
            plt.title('Average Minimum Temperature, All Stations '+start+'-'+end)
            plt.margins(0.1)
            plt.tight_layout()
            for item in range(len(tempmin_values)):
                plt.text(item, tempmin_values[item], tempmin_values[item], ha='center', rotation=0, verticalalignment='center_baseline')
            plt.xlabel('Station', fontweight='bold', color = 'black', fontsize='12')
            plt.ylabel('Min Temp (°F)', fontweight='bold', color = 'black', fontsize='12')     
            
            start = start.replace('/', '.')
            end = end.replace('/', '.')            
            
            plt.savefig('Western_PA_Climate_Project/Plots/All_Stations/Means_Comparisons/Temp_Min_All_Stations-'+start+'-'+end+'.jpg')
            print('\nFigure saved to directory \'Plots/All_Stations/Means_Comparisons\'')
            plt.close()

        if av_var_choice == '4':
          
            tempmax_values = []
            tempmax_keys = []
            
            for value in tempmax_dict.values():
                value = round(value, 1)
                tempmax_values.append(value)
                
            mean_tempmax = (sum(tempmax_values))/(len(tempmax_values))
            mean_tempmax = round(mean_tempmax, 1)
            tempmax_values.append(mean_tempmax)     
                
            for key in tempmax_dict.keys():
                tempmax_keys.append(key)        
                
            tempmax_keys.append('All Stations Average')    
                
            plt.bar(tempmax_keys, tempmax_values, color = 'firebrick')
            plt.xticks(rotation =60, fontsize = 10, ha = 'right')
            plt.title('Average Maximum Temperature, All Stations '+start+'-'+end)
            plt.margins(0.1)
            plt.tight_layout()
            for item in range(len(tempmax_values)):
                plt.text(item, tempmax_values[item], tempmax_values[item], ha='center', rotation=0, verticalalignment='center_baseline')
            plt.xlabel('Station', fontweight='bold', color = 'black', fontsize='12')
            plt.ylabel('Max Temp (°F)', fontweight='bold', color = 'black', fontsize='12')         
            
            start = start.replace('/', '.')
            end = end.replace('/', '.')     
            
            plt.savefig('Western_PA_Climate_Project/Plots/All_Stations/Means_Comparisons/Temp_Max_All_Stations-'+start+'-'+end+'.jpg')
            print('\nFigure saved to directory \'Plots/All_Stations/Means_Comparisons\'')
            plt.close()
        
    else:
        print('\nInput not recognized.  Returning to Main Menu.')
        menu()
   
#This is the main attraction; we pick a variable and plot it across the specified date range for all stations.  Lots of data getting processed for this one.                
def comparison_plotter():
     
    df_dict, start, end = slice_dfs()
    df_keys = df_dict.keys()
    
    var_sel = integer_checker('Select a variable: \n1. Average Daily Snowfall\n2. Average Daily Snow Depth\n3. Average Daily Minimum Temperature\n4. Average Daily Maximum Temperature\n\nChoice: ')
    
    var_sel_choices = {1:'Snowfall', 2:'Snowdepth', 3:'TempMin', 4:'TempMax'}
    var_sel_keys = var_sel_choices.keys()
                       
    if var_sel in var_sel_keys:
        var_sel = var_sel_choices[var_sel]
        
    else:
        print('Choice not valid.  Returning to Main Menu.')
        menu()

    #This for loop redefines the values in the df_dict(ionary) as the series corresponding to whichever weather variable corresponds to the chosen var_sel value.  So, each key is a station name, and each value is now a series corresponding to var_sel.
    for key in df_keys:
        df_dict[key] = pd.Series(df_dict[key][var_sel], name = key)
        
    #We make an emptyy dataframe that we'll now merge all of the series into, matching them on the index; the result is a df with ObDate as the index, the stations names as the columns, and the values from each individual for var_sel (a weather variable, like snowfall in inches, etc.) as the values in each cell.
    
    #Make the empty df
    all_stations_df = pd.DataFrame()
    
    #Merge all our series into it.  It's an outer merge, which I grok from SQL; left _index and right_index being true means that we are matching indexes where possible and adding them to the left(there'll only be one index in the final df) where there is no match.
    for key, value in df_dict.items():
        value = pd.Series(value)
        all_stations_df = all_stations_df.merge(value, how='outer', left_index=True, right_index=True)
     
    #The by-now familiar process of producing "pretty printable" versions of the weather variables.    
    fixed_var_sels = {'Snowdepth':'Daily Snow Depth', 'Snowfall':'Daily Snowfall', 'TempMin':'Daily Minimum Temperature', 'TempMax':'Daily Maximum Temperature'}
    
    #And we sort var_sel into two classes to determine what units to display on the plot.
    if var_sel == 'TempMin' or var_sel == 'TempMax':
        unit = ' (Degrees Fahrenheit)'
    else:
        unit = ' (inches)'              

    #Here we convert the index from string to datetime, which is a handy-dandy built-in feature of Pandas...
    all_stations_df.index = pd.to_datetime(all_stations_df.index)
    #And then just to be safe, we sort the index, which by default will be chronologically for this type of object
    all_stations_df = all_stations_df.sort_index()
    
    #And create this variable, a tally of the total number of observations graphed, to be displayed on the x-axis.  Since days with snowdepth = 0 were excluded from the dataset from the start, this number is in itself fairly informative.
    ob_count = str(len(all_stations_df.index))

    #So we finally get to plot our dataframe; the figure size is quite large, since we're dealing with a scatter plot with thousands of observations over nearly fifty years.
    
    color_dict = {'Snowdepth': np.array(["darkgoldenrod","seagreen","goldenrod","darkgoldenrod","darkseagreen","darkkhaki","khaki","forestgreen","wheat","moccasin","olivedrab","green","yellowgreen", "olive"]), 'TempMin':np.array(['powderblue', 'lightblue', 'deepskyblue', 'skyblue', 'steelblue', 'dodgerblue', 'blue', 'cornflowerblue', 'royalblue', 'lightskyblue', 'silver', 'linen', 'mediumblue', 'lightsteelblue']), 'Snowfall':np.array(['mediumpurple', 'rebeccapurple', 'blueviolet', 'indigo', 'darkorchid', 'thistle', 'plum', 'fuchsia', 'orchid', 'hotpink', 'palevioletred', 'pink', 'magenta', 'mediumvioletred']), 'TempMax':np.array(['lightcoral', 'indianred', 'brown', 'firebrick', 'maroon', 'red', 'salmon', 'orangered', 'sienna', 'chocolate', 'darkorange', 'orange', 'gold', 'crimson'])}
    
    if var_sel in color_dict:
        colors = color_dict[var_sel]
    
    all_stations_plot = all_stations_df.plot(style='.', ms=12, figsize = (25, 10), fontsize=18, color=colors)
    
    #Set title, fontsize;
    plt.title('Historical '+fixed_var_sels[var_sel]+'\n', fontsize=24)
    #Set xlabel and ylabel, fontsizes.
    plt.xlabel('\nObservations = '+ob_count, fontsize=18)
    plt.ylabel(fixed_var_sels[var_sel]+unit+'\n', fontsize=18)
    
    #And fix these once again for filepath-ing
    start = start.replace('/', '.')
    end = end.replace('/', '.')
    
    #We don't want to display this huge plot, so we handle things this way instead, as above, and save the plot to the appropriate directory.
    fig = all_stations_plot.get_figure()
    fig.savefig('Western_PA_Climate_Project/Plots/All_Stations/All_Stations_'+var_sel+'_'+start+'-'+end+'.png')
    plt.close()
    print('\nPlot saved in directory \'Plots/All_Stations\' with appropriate variable names.')
    #And just because they might be handy, we save the descriptive statistics to csv as well.
    #First, we'll create a new colum to add to the df, one comprised of the averages for each station for each row in the df.
    all_stations_summary_df = all_stations_df.describe()
    
    counts = list(all_stations_summary_df.loc['count'])
    means = list(all_stations_summary_df.loc['mean'])
    stds = list(all_stations_summary_df.loc['std'])
    mins = list(all_stations_summary_df.loc['min'])
    twentyfives = list(all_stations_summary_df.loc['25%'])
    fifties = list(all_stations_summary_df.loc['50%'])
    seventyfives = list(all_stations_summary_df.loc['75%'])
    maxes = list(all_stations_summary_df.loc['max'])

    count = round((sum(counts))/(len(counts)), 1)
    mean = (sum(means))/(len(means))
    std = (sum(stds))/(len(stds))
    mins = (sum(mins))/(len(mins))
    twentyfive = round((sum(twentyfives))/(len(twentyfives)), 1)
    fifty = round((sum(fifties))/(len(fifties)), 1)
    seventyfive = round((sum(seventyfives))/(len(seventyfives)), 1)
    maxes = round((sum(maxes))/(len(maxes)), 1)
    
    all_stations_list = [count, mean, std, mins, twentyfive, fifty, seventyfive, maxes]
    
    all_stations_summary_df['All Stations Average'] = all_stations_list                     

    all_stations_summary_df.to_csv('Western_PA_Climate_Project/Summary_Stats/All_Stations_'+var_sel+'_'+start+'-'+end+'.csv')
    print('Descriptive statistics saved in directory \'Summary_Stats\'.')

#For this function, we take the same mean values used in the descriptive_stats_grapher and plot each/any one against the geographical characteristics of each station:  latitude, longitude, elevation, and a weighted average of latitude and elevation (more on that below). 
def correlation_plotter():
    #First bit is the same as in the descriptive stats grapher...
    df_dict, start, end = slice_dfs()
    df_keys = df_dict.keys()
    
    for key in df_keys:
        df_dict[key] = df_dict[key].describe()

    for key in df_keys:
        df_dict[key] = pd.Series(df_dict[key].loc['mean'])
        
    snowfall_dict = {}
    snowdepth_dict = {}
    tempmin_dict = {}
    tempmax_dict = {}
    
    for key in df_keys:
        snowfall_dict[key] = df_dict[key].loc['Snowfall']
        snowdepth_dict[key] = df_dict[key].loc['Snowdepth']
        tempmin_dict[key] = df_dict[key].loc['TempMin']      
        tempmax_dict[key] = df_dict[key].loc['TempMax']        
    
    #Here we do something different, though; we finally want to access our stations spreadsheet, so we can grab that geographical information.
    stations_df = pd.read_csv('Western_PA_Climate_Project/Climate_Data/Stations/Stations.csv')

    stations_df.set_index('StationName', inplace=True)
    stations_df.sort_index(inplace=True)

    #And we add four columns, one for each weather variable, to the stations dataframe, just by calling up the values from the previous dictionary; both are sorted alphabetically  to ensure that the values will correspond to the appropriate station.
    stations_df['Snowfall'] = sorted(snowfall_dict.values())
    stations_df['Snowdepth'] = sorted(snowdepth_dict.values())    
    stations_df['TempMin'] = sorted(tempmin_dict.values())
    stations_df['TempMax'] = sorted(tempmax_dict.values())   

    #Build a series of just the mean values for each weather variable for each station
    means_series = stations_df.describe().loc['mean']
    
    #We only need these variables (mean latitude and mean elevation for the set of stations) so that we can calculate our weighted average for latitude and elevation; as we'll see below, this is calculated by computing a z-score (standardized distance from the mean) for each, which removes the bias towards elevation that otherwise would have existed.  Once we have a z-score for each, we'll average the two and assign that to each station.
    lat_mean = means_series.loc['Latitude']
    elev_mean = means_series.loc['Elevation']
    
    #Make a list of all the latitude and all the longitude values, to be iterated over in our calculations of the z-scores for each.
    lat_list = stations_df['Latitude'].to_list()
    elev_list = stations_df['Elevation'].to_list()
    
    #Grab the standard deviations for latitude and elevation (need 'em to get the z-scores)
    lat_sd = stations_df.describe().at['std', 'Latitude']
    elev_sd = stations_df.describe().at['std', 'Elevation']
    
    #Some empty lists in which to put our z-scores via list.append()
    lat_z_scores = []
    elev_z_scores = []
    
    #Take each lat, calculate its z-score, stick it into the z-score list for latitudes...
    for lat in lat_list:
        lat_z=(lat-lat_mean)/lat_sd
        lat_z_scores.append(lat_z)
    #And do just the same thing for elevation...    
    for elev in elev_list:
        elev_z=(elev-elev_mean)/elev_sd
        elev_z_scores.append(elev_z)    
    #And then average the two z-scores to get our 'weighted average'
    combined_z_scores = [(a + b)/2 for a, b in zip(lat_z_scores, elev_z_scores)]
    #Add the weighted z's into the dataframe as the 'Lat/El' column...    
    stations_df['Lat/El'] = combined_z_scores

    #And we're ready to proceed:  these are the user's choices for the x axis.
    x_choices = {1:'Latitude', 2:'Longitude', 3:'Elevation', 4:'Lat/El'}

    x_choice = integer_checker('Choose a variable to plot on the x axis: \n\n1.Latitude\n2.Longitude\n3.Elevation\n4.Weighted Average for Latitude and Elevation\n\nChoice: ')

    if x_choice in x_choices.keys():
        x_chosen = x_choices[x_choice]
    else:
        print('Not a valid choice. Returning to Main Menu.')
        menu()
    
    #user choices for y-axis of correlation plotter    
    y_choices = {1:'Snowfall', 2:'Snowdepth', 3:'TempMin', 4:'TempMax'}        
    y_choice = integer_checker('Choose a variable to plot on the y axis: \n\n1.Mean Daily Snowfall\n2.Mean Daily Snow Depth\n3.Mean Daily Minimum Temperature\n4.Mean Daily Maximum Temperature\n\nChoice: ')
    
    if y_choice in y_choices.keys():
        y_chosen = y_choices[y_choice]
    else:
        print('Not a valid choice. Returning to Main Menu.')
        menu()
    
    #Build two lists for plotting, using the chosen variables.  Might not need to cast them to lists, but it seemed safest and simplest.    
    x = stations_df[x_chosen].to_list()
    y = stations_df[y_chosen].to_list()
    
    #Create a list of names from our df's index, to be used in assigning names to our plotted points later
    names = stations_df.index.to_list()
    
    #Just setting the units (x_units, y_units) and the labels (x_chosen, y_chosen) for display in the plot - pretty straightforward.
    if x_chosen == 'Latitude':
        x_units = ' (°N)'
    if x_chosen =='Longitude':
        x_units = ' (°W)'
    if x_chosen == 'Elevation':
        x_units = ' (feet above sea level)'    
    if x_chosen == 'Lat/El':
        x_units = ''
        x_chosen = 'Weighted Average for Latitude and Elevation (Z-score)'
    if y_chosen == 'Snowfall':
        y_units = ' (inches)'
    if y_chosen == 'Snowdepth':
        y_units = ' (inches)'
        y_chosen = 'Snow Depth'
    if y_chosen == 'TempMin' or y_chosen == 'TempMax':
        y_units = ' (degrees Fahrenheit)'

    #Set figure size - not a data-heavy plot, so fairly small is sufficient
    plt.figure(figsize=(6, 4))
    #And now to actually plot it; first two arguments are the lists for plotting against one another, s is the dot size.  Easy Peasy.
    plt.scatter(x, y, s=10)
    
    #Create a custom title for our plot, one that'll be unique.
    title = x_chosen+' v. '+y_chosen+' Correlation Plot ('+start+'-'+end+')'
    
    #Build the labels for the plot
    plt.xlabel(x_chosen+x_units)
    plt.ylabel('Mean '+y_chosen+y_units)
    #And set the title, as determined above.
    plt.title(title)
    
    #As usual by now, get rid of the slashes for filepath purposes.
    start = start.replace('/', '.')
    end = end.replace('/', '.')
    
    #And generate the title anew without slashes, so as for to save it.
    title = x_chosen+' v. '+y_chosen+' Correlation Plot ('+start+'-'+end+')'
    
    #And what was the trickiest part of this, annotating the points using matplotlib's annotate feature.  First argument is string to annotate with, then x/y corresponence (location of point on plot).
    for point in range(len(x)):
        plt.annotate(names[point], (x[point], y[point]))
    #Tight layout for safety's sake
    plt.tight_layout()
    #Save it
    plt.savefig('Western_PA_Climate_Project/Plots/Correlation_Plots/'+title+'.jpg')
    print('\nPlot saved to \'Plots/Correlation_Plots\'')
    #And close it up
    plt.close()

#And finally, the one function to rule them all, to find them and in the darkness bind them:  the menu function.  This one is pretty straightforward; each of the above functions is mapped to a menu sleection contained within an if clause.  
def menu():
    
    #This is not strictly necessary, since the user will see this list in some of the other functions, but in case they're curious when they first run the program, this will tell them the locations of the meteorological stations used.
    stations_df = pd.read_csv('Western_PA_Climate_Project/Climate_Data/Stations/Stations.csv')
    station_list = stations_df['StationName'].to_list()    
    
    #List of options
    menu_choice = input('1. Pick date range (last used is default)\n2. Plot Precipitation and Snowfall Variables for a single station\n3. Compare averages across stations\n4. Compare a variable across all stations\n5. Plot correlations between station location and temperature and precipitation \n6. View list of stations\n7. Show current start and end dates\n8. Quit (Q/q)\n\nChoice: ')
    #python list of options, really.
    menu_choices = ['1', '2', '3', '4', '5', '6', '7', '8']
    #This should all be pretty self-evident
    if menu_choice in menu_choices:
        
        if menu_choice == '1':
            get_start_date()
            get_end_date()
            menu()
        if menu_choice == '2':
            single_df_plotter()
            menu()
        if menu_choice == '3':
            descriptive_stats_grapher()
            menu()
        if menu_choice == '4':
            comparison_plotter()
            menu()
        if menu_choice == '5':
            correlation_plotter()
            menu()
        if menu_choice == '6':
            print('\nAll Stations:\n')
            for station in station_list:
                print(station)
            print('\nStation Count: '+str(len(station_list)))    
            menu()
        if menu_choice == '7':
            show_slicer_dates()
            menu()
        if menu_choice == '8':
            func_quit()
            
    elif menu_choice == 'Q' or menu_choice == 'q':
        func_quit()
        
    else:
        print('\nInput not recognized.  Try again.')
        menu()
        
def intro():
    print('\nWelcome to the Western Pennsylvania Winter Weather Climate Plotter.  This program draws on a database of 30,161 daily weather observations, spread (somewhat unevenly) across 14 weather stations located in Western Pennsylvania between the Allegheny Front and the Ohio Border.  A list of stations can be accessed from the menu below.\n\nAll data has been retrieved via email link from https://www.ncdc.noaa.gov/, by individual request per station. The source code for this program is available at https://github.com/silfeid/fa21python2_adam, as are the records in .csv format upon which it draws.  Filepaths are relative but readily adaptable. Please address any comments or questions to brodeam@gmail.com.')

def main():

    download_files()
    move_stations()
    make_project_directories()
    set_initial_dates()
    intro()
    menu()
    
    
if __name__ == "__main__":
    main()