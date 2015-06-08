---------------------
YELP API Search Engine
---------------------
I. TOC
	a.Overview (II)
	b.Set Up and Requirements (III)

II. OVERVIEW

	This is an script which allows anyone to search the yelp API and get everything that they have. Basically you input a <search term> and a <location> and it outputs a CSV file of everything that YELP has about business' about this term in the corresponding location. This does require you to fiddle around with the terminal but I will tell you exactly what to do so you will be okay.

	A <term> is defined as a type of business. E.g bars,restaurants, diners, taco shops, etc.

	A <location> is any geographical identifier, but it seems YELP best works if you go by city. E.g Pittsburgh, Phoenix, Moraga.

III. Set Up and Requirements

	I have emailed you all the python script which is labelled PubFinder.py. I want you to put it in your Desktop file. (Drag and drop to your desktop)

	1. Open spotlight and search for the terminal application. Then open it. You should see a white window pop up.
	
	2. Now you should be in the user dicrectory. Type "cd Desktop". This tells the terminal to change directory to the desktop folder. You are basically navigating to the desktop folder without using a mouse which you would normally do using finder. 
	
	3. Now that you are in the folder with the PubFinder.py file. You are ready to run it. Pick a term and a location you want to get Yelp data from E.g Bars,Pittsburgh or Taco Shops,Phoenix.
	
	4. Type the following: python PubFinder.py <term you thought of> <location you thought of> 

	5. Press enter and it will start running. This takes time as you have to make a ton of API calls to the yelp server and get about 4000 rows of data and parse them all.
	
	6. When it is done. Type ls into the terminal.
	
	7. Now if you close the terminal app and go to your desktop, you should see a output.txt file and a output.csv file. The txt file has the raw json formats and the output.csv file has a flat tabular representation of the data which you can port over to microsoft excell for further analysis. 
	
	8. Congrats you are done!

Please email abhinavasingh16@gmail.com if you have any questions!


