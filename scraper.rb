require 'nokogiri'
require 'open-uri'
require 'sqlite3'
require 'logger'
require 'date'

# Set up a logger to log the scraped data
logger = Logger.new(STDOUT)

# Define the URL of the page
url = 'https://www.wtc.tas.gov.au/Your-Property/Planning/Currently-Advertised-Planning-Applications'

# Step 1: Fetch the page content for the main listing
begin
  logger.info("Fetching page content from: #{url}")
  page_html = open(url).read
  logger.info("Successfully fetched page content.")
rescue => e
  logger.error("Failed to fetch page content: #{e}")
  exit
end

# Step 2: Parse the page content using Nokogiri
doc = Nokogiri::HTML(page_html)

# Step 3: Initialize the SQLite database
db = SQLite3::Database.new "data.sqlite"

# Create table if it doesn't already exist
db.execute <<-SQL
  CREATE TABLE IF NOT EXISTS westtamar (
    id INTEGER PRIMARY KEY,
    description TEXT,
    date_scraped TEXT,
    date_received TEXT,
    on_notice_to TEXT,
    address TEXT,
    council_reference TEXT,
    applicant TEXT,
    owner TEXT,
    stage_description TEXT,
    stage_status TEXT,
    document_description TEXT,
    title_reference TEXT
  );
SQL

# Define variables for storing extracted data for each entry
address = ''  
description = ''
on_notice_to = ''
title_reference = ''
date_received = ''
council_reference = ''
applicant = ''
owner = ''
stage_description = ''
stage_status = ''
document_description = ''
date_scraped = Date.today.to_s

logger.info("Start Extraction of Data")

# Loop through all the planning application items on the main page
doc.css('.edn_article').each_with_index do |item, index|
  # Extract the council reference (PA NO)
  council_reference = item.at_css('.edn_articleTitle a') ? item.at_css('.edn_articleTitle a').text.strip.sub('PA NO:', '').strip : 'NA'

  # Extract the applicant (from the applicant subtitle)
  applicant = item.at_css('.edn_articleTitle.edn_articleSubTitle') ? item.at_css('.edn_articleTitle.edn_articleSubTitle').text.sub('APPLICANT:', '').strip : 'NA'

  # Extract the description, which is the proposal part of the text
  description_raw = item.at_css('.edn_articleSummary') ? item.at_css('.edn_articleSummary').text.strip.sub('PROPOSAL:', '').strip : 'NA'
  description = description_raw.split('LOCATION:').first.strip
  
  # Extract the location (from the article summary)
  address = item.at_css('.edn_articleSummary') ? item.at_css('.edn_articleSummary').text.split('LOCATION:').last.split('CLOSES:').first.strip : 'NA'

  # Extract the closing date from the article summary
  on_notice_to_raw = item.at_css('.edn_articleSummary') ? item.at_css('.edn_articleSummary').text.split('CLOSES:').last.strip : 'NA'
  on_notice_to_date = on_notice_to_raw.sub(/\D*\d{1,2}(\w+)\s*(\d{1,2}\w{2})\s*(\d{4})/, '\2 \1 \3') 
  begin
    on_notice_to = Date.parse(on_notice_to_date).strftime('%Y-%m-%d')
  rescue ArgumentError
    on_notice_to = 'Invalid'
  end

  # Extract the date received from the <time> element
  date_received_raw = item.at_css('time') ? item.at_css('time').text.strip : 'Date not found'
  begin
    date_received = Date.parse(date_received_raw).strftime('%Y-%m-%d')
  rescue ArgumentError
    date_received = 'Invalid'
  end

  # Extract the link to the detailed page
  application_url = item.at_css('.edn_articleTitle a')['href'] if item.at_css('.edn_articleTitle a')

  # Log the extracted data
  logger.info("Council Reference: #{council_reference}")
  logger.info("Applicant: #{applicant}")
  logger.info("Description RAW: #{description_raw}")
  logger.info("Description: #{description}")
  logger.info("Address: #{address}")
  logger.info("Closing Date: #{on_notice_to}")
  logger.info("Closing Date: #{date_received_raw}")
  logger.info("View Details Link: #{application_url}")
  logger.info("-----------------------------------")

  # Step 4: Ensure the entry does not already exist before inserting
  existing_entry = db.execute("SELECT * FROM westtamar WHERE council_reference = ?", council_reference)

  if existing_entry.empty?  # Only insert if the entry doesn't already exist
    # Save data to the database
    db.execute("INSERT INTO westtamar 
      (council_reference, applicant, description, address, date_received, on_notice_to, date_scraped) 
      VALUES (?, ?, ?, ?, ?, ?, ?)",
      [council_reference, applicant, description, address, date_received, on_notice_to, date_scraped])

    logger.info("Data for #{council_reference} saved to database.")
  else
    logger.info("Duplicate entry for document #{council_reference} found. Skipping insertion.")
  end
end

# Finish
logger.info("Data has been successfully inserted into the database.")
