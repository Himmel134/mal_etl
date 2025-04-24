
# Scraping and Visualizing Top Anime Data from MyAnimeList

## 1. Project Overview 📌  
This project automates data extraction of top anime from [MyAnimeList](https://myanimelist.net) using web scraping techniques. It collects key information such as anime titles, scores, number of members, genres, and studios. The data is then cleaned, visualized, and analyzed to uncover trends in popular anime series.

This project helps anime fans, data enthusiasts, and content curators better understand what makes an anime popular based on rating metrics and member engagement. The final insights can be further visualized using tools like Looker Studio or Tableau.

---

## 2. Project Description  
The data in this dataset was collected through web scraping using `requests`, `BeautifulSoup`, and `pandas`. The scraper targets the “Top Anime” list on MyAnimeList to gather relevant information from multiple pages.

### Process Overview:
- **Scraping:** Extract data such as title, score, members, genre, and studios from each page of the Top Anime list.
- **Cleaning:** Remove or handle missing values, convert numeric strings, and separate genre information for analysis.
- **Visualization:** Use pandas and matplotlib to generate insightful visualizations about the most popular anime.
- **Analysis:** Identify patterns and key trends in the most-watched or highest-rated anime series.

---

## 3. Dataset Column Descriptions  
| Column              | Description                                                                 |
|---------------------|-----------------------------------------------------------------------------|
| **Title**           | Name of the anime (e.g., *Fullmetal Alchemist: Brotherhood*)               |
| **Score**           | Average rating given by users on MyAnimeList (e.g., 9.15)                   |
| **Members**         | Number of members (users who added the anime to their list)                 |
| **Type**            | Format of the anime (e.g., TV, Movie, OVA)                                  |
| **Episodes**        | Number of episodes                                                          |
| **Genres**          | List of genres associated with the anime (e.g., Action, Adventure)          |
| **Studios**         | Name(s) of the production studio(s)                                         |
| **Aired**           | Airing date range or release year                                           |
| **Rank**            | Position in the MyAnimeList ranking                                         |

---

## 4. Requirements  
Install required libraries using the following command:

```bash
pip install -r requirements.txt
```

If `requirements.txt` is not present, install these manually:

```bash
pip install pandas matplotlib seaborn requests beautifulsoup4
```

---

## 5. Project Structure  

```
├── best_anime.ipynb                  # Main notebook for scraping, cleaning, and visualizing
├── data/
│   └── top_anime.csv                 # (Optional) Saved dataset of the scraped anime data
├── figures/
│   └── *.png                         # (Optional) Generated plots
├── README.md                         # This README file
└── requirements.txt                  # Required libraries
```

---

## 6. How to Use  
1. Open the `best_anime.ipynb` notebook.
2. Run the notebook cells in order:
   - Scrapes the data from MyAnimeList.
   - Cleans and prepares the data.
   - Visualizes statistics and distributions (e.g., most common genres, scores, etc.).
3. Export or save the dataset and plots if desired.

---

## 7. Key Functions in the Notebook  

- `get_anime_data()`: Scrapes anime title, score, members, and other attributes.
- `clean_anime_data()`: Cleans and preprocesses the scraped dataset.
- `plot_top_genres()`: Visualizes the most common anime genres.
- `plot_score_distribution()`: Shows distribution of anime scores.
- `correlation_analysis()`: Examines the relationship between number of members and score.

---

## 8. Contact  

**Nama**: Jawed Iqbal Alfaruqiy
**Email**: jawediqbalalfaruqiy@gmail.com  
**LinkedIn**: [linkedin.com/in/jawed-iqbal](https://www.linkedin.com/in/jawed-iqbal)
