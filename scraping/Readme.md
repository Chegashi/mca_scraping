scraping/                                   # Root directory for the scraping project.
├── dags/                                   # Contains Directed Acyclic Graphs (DAGs) for Airflow.
│   ├── daily_scraping_dag.py               # Defines the DAG for daily scraping tasks.
│   └── monthly_scraping_dag.py             # Defines the DAG for monthly scraping tasks.
├── scrapers/                               # Contains individual scraping scripts for each targeted website.
│   ├─── utility.py                         # Utility functions used across different scrapers.
│   ├── Anapec_scraper.py                   # Scraper script for the Anapec website.
│   ├── Critmaroc_scraper.py                # Scraper script for the Critmaroc website.
│   ├── Emploima_scraper.py                 # Scraper script for the Emploima website.
│   ├── Marocadre_scraper.py                # Scraper script for the Marocadre website.
│   ├── Mjob_scraper.py                     # Scraper script for the Mjob website.
│   ├── Optioncarriere_scraper.py           # Scraper script for the Optioncarriere website.
│   └── Rekrute_scraper.py                  # Scraper script for the Rekrute website.
├── processors/                             # Contains scripts for processing the scraped data.
│   ├── utilities/                          # Utilities specific to data processing tasks.
│   │   ├── __init__.py                     # Initializes the processors' utilities as a Python package.
│   │   └── utility.py                      # Utility functions for data processing tasks.
│   ├── daily_processor.py                  # Processes data scraped on a daily basis.
│   └── monthly_processor.py                # Processes data scraped on a monthly basis.
├── data/                                   # Central directory for all data related to the project.
│   ├── raw/                                # Unprocessed data collected from the scrapers.
│   │   ├── daily/                          # Raw data organized by the day it was scraped.
│   │   │   ├── Anapec/                     # Daily raw data from Anapec.
│   │   │   ├── Critmaroc/                  # Daily raw data from Critmaroc.
│   │   │   ├── Emploima/                   # Daily raw data from Emploima.
│   │   │   ├── Marocadre/                  # Daily raw data from Marocadre.
│   │   │   ├── Mjob/                       # Daily raw data from Mjob.
│   │   │   ├── Optioncarriere/             # Daily raw data from Optioncarriere.
│   │   │   └── Rekrute/                    # Daily raw data from Rekrute.
│   │   └── monthly/                        # Raw data organized by the month it was scraped.
│   |       ├── Anapec/                     # Monthly raw data from Anapec
│   |       ├── Critmaroc/                  # Monthly raw data from Critmaroc  
│   |       ├── Emploima/                   # Monthly raw data from Emploima  
│   |       ├── Marocadre/                  # Monthly raw data from Marocadre 
│   |       ├── Mjob/                       # Monthly raw data from Mjob  
│   |       ├── Optioncarriere/             # Monthly raw data from Optioncarriere
│   |       └── Rekrute/                    # Monthly raw data from Rekrute
│   ├── parquet/                            # Directory for data stored in Parquet format, an efficient binary format.
│   │   ├── daily/                          # Daily data converted to Parquet format for optimized storage.
│   │   │   ├── Anapec/                     # Daily Parquet data for Anapec.
│   │   │   ├── Critmaroc/                  # Daily Parquet data for Critmaroc.
│   │   │   ├── Emploima/                   # Daily Parquet data for Emploima.
│   │   │   ├── Marocadre/                  # Daily Parquet data for Marocadre.
│   │   │   ├── Mjob/                       # Daily Parquet data for Mjob.
│   │   │   ├── Optioncarriere/             # Daily Parquet data for Optioncarriere.
│   │   │   └── Rekrute/                    # Daily Parquet data for Rekrute.
│   │   └── monthly/                        # Monthly data converted to Parquet format.
│   |       ├── Anapec/                     # Monthly Parquet data for Anapec.
│   |       ├── Critmaroc/                  # Monthly Parquet data for Critmaroc.
│   |       ├── Emploima/                   # Monthly Parquet data for Emploima.
│   |       ├── Marocadre/                  # Monthly Parquet data for Marocadre.
│   |       ├── Mjob/                       # Monthly Parquet data for Mjob.
│   |       ├── Optioncarriere/             # Monthly Parquet data for Optioncarriere.
│   |       └── Rekrute/                    # Monthly Parquet data for Rekrute.
│   ├── processed/                          # Processed and cleaned data ready for analysis.
│   │   ├── utilities/                      # Utilities for processing data.
│   │   │   ├── __init__.py                 # Initializes the directory as a Python package.
│   │   │   └── utility.py                  # Contains utility functions for data processing.
│   │   ├── daily/                          # Data processed on a daily basis.
│   │   │   └── daily_processing.py         # Script for daily data processing (might need adjustment to directory structure).
│   │   └── monthly/                        # Data processed on a monthly basis.
│   │       └── monthly_processing.py       # Script for monthly data processing.
│   ├── history/                            # Records of historical data for tracking and analysis.
│   │   ├── scraping_history_daily.csv      # Log of daily scraping activities.
│   │   ├── scraping_history_daily_acc.csv  # Accumulated log of daily scraping activities.
│   │   └── scraping_history_monthly.csv    # Log of monthly scraping activities.
├── monitoring/                             # Scripts and outputs for monitoring the scraping and processing operations.
│   ├── plots/                              # Visualization scripts and their generated plots.
│   │   ├── plotting_scripts_daily.py       # Scripts for generating daily plots.
│   │   ├── plotting_scripts_monthly.py     # Scripts for generating monthly plots.
│   │   ├── plotting_utility.py             # Utility functions for plotting.
│   │   ├── daily/                          # Stored daily plots.
|   │   │   ├── Anapec_progress.png         # Visualization of daily scraping  progress for the Anapec website
│   │   │   ├── Critmaroc_progress.png      # Visualization of daily scraping  progress for the Critmaroc website
│   │   │   ├── Emploima_progress.png       # Visualization of daily scraping  progress for the Emploima website
│   │   │   ├── Marocadre_progress.png      # Visualization of daily scraping  progress for the Marocadre website
│   │   │   ├── Mjob_progress.png           # Visualization of daily scraping  progress for the Mjob website
│   │   │   ├── Optioncarriere_progress.png # Visualization of daily scraping  progress for the Optioncarriere website
│   │   │   └── Rekrute_progress.png        # Visualization of daily scraping  progress for the Rekrute website
│   │   ├── monthly/                        # Stored monthly plots.
│   │   |   ├── Anapec_progress.png         # Visualization of daily scraping  progress for the Anapec website
│   │   |   ├── Critmaroc_progress.png      # Visualization of daily scraping  progress for the Critmaroc website
│   │   |   ├── Emploima_progress.png       # Visualization of daily scraping  progress for the Emploima website
│   │   |   ├── Marocadre_progress.png      # Visualization of daily scraping  progress for the Marocadre website
│   │   |   ├── Mjob_progress.png           # Visualization of daily scraping  progress for the Mjob website
│   │   |   ├── Optioncarriere_progress.png # Visualization of daily scraping  progress for the Optioncarriere website
│   │   |   └── Rekrute_progress.png        # Visualization of daily scraping  progress for the Rekrute website
│   │   └── reports/                        # Generated reports from monitoring data.
│   │         └── report.png                # Example report image.
├── logs/                                   # Root directory for log files generated by the scraping system.
│   ├── scrapers/                           # Log files related to the scraping tasks.
│   │   ├── Anapec_YYYYMMDD.log             # Log file for the Anapec scraper, containing details of the scraping session on a specific date (YYYYMMDD).
│   │   ├── Critmaroc_YYYYMMDD.log          # Log file for the Critmaroc scraper, documenting the scraping activity and any issues encountered on a specific date.
│   ├── processors/                         # Log files related to the data processing tasks.
│   │   ├── daily_processing_YYYYMMDD.log   # Log file documenting the daily data processing activities, including any errors or statistics, on a specific date.
│   │   ├── monthly_processing_YYYYMMDD.log # Log file for monthly data processing tasks, capturing the processing details and outcomes for a specific month.
│   └── monitoring/                         # Log files generated by monitoring scripts or tools.
│       ├── daily_monitoring_YYYYMMDD.log   # Log file containing monitoring information (e.g., system health, performance metrics) for a specific day.
│       ├── monthly_monitoring_YYYYMMDD.log # Monthly monitoring log, providing an overview of long-term trends or issues detected during the month.
├── scraper_config.ini                      # Configuration settings for scrapers.
├── requirements.txt                        # pip requirements file for dependencies.
├── .caches/                                # Caching directory for storing temporary cache files.
│   ├── Anapec                              # Cache files specific to Anapec scraper.
│   ├── Critmaroc                           # Cache files specific to Critmaroc scraper.
│   ├── Emploima                            # Cache files specific to Emploima scraper.
│   ├── Marocadre                           # Cache files specific to Marocadre scraper.
│   ├── Mjob                                # Cache files specific to Mjob scraper.
│   ├── Optioncarriere                      # Cache files specific to Optioncarriere scraper.
│   └── Rekrute                             # Cache files specific to Rekrute scraper.
└── README.md                               # Documentation file for the project, including setup and usage instructions.
