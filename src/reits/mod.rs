
pub mod reit_data{
    use scraper::selector::{Selector};
    use selectors::parser::SelectorParseErrorKind;
    use polars::frame::DataFrame;
    use polars::prelude::*;
    use std::sync::Arc;
    use scraper::html::{Html};
    use polars::prelude::Expr;
    use polars::df;
    use polars::export::rayon::iter::ParallelIterator;
    use polars_sql::SQLContext;
    static REITTICKERS: [&str;15] = ["SVC", "RHP", "HST", "APLE", "SHO", "SOHO", "PK", "DRH",
                          "BHR", "AHT", "HT", "PEB", "XHR", "INN", "CLDT"];

    #[derive(Debug)]
    pub enum ParsError<'a> {
        ReqError(reqwest::Error),
        SerialError(serde_json::Error),
        ParseError(cssparser::ParseError<'a, SelectorParseErrorKind<'a>>),
    }

    impl From<reqwest::Error> for ParsError<'_> {
        #[inline]
        fn from(err: reqwest::Error) -> ParsError<'static> {
            return ParsError::ReqError(err);
        }
    }
    
    pub async fn query_ticker(tick_vec: Vec<String>, dat: DataFrame) -> Option<Vec<LazyFrame>>{
        let tick_vec = Arc::new(tick_vec);
        let mask = dat["RET"].utf8().unwrap().contains_literal("C").unwrap();
        let dat = dat.filter(&!&mask).unwrap().sort(["TICKER","date"],true).unwrap().lazy();
        
        println!("{:?}",&dat.clone().select([count()]).collect().unwrap());
        println!("{:?}",&dat.clone().collect().unwrap().head(Some(10)));
        
        let mut ctx = SQLContext::new();                                                                                                                  
        ctx.register("df",dat);
        let mut handles = Vec::new();
        for i in 0..=tick_vec.len() {
            let mut temp_ctx = ctx.clone();
            let temp_tick = tick_vec[i].clone();
            let handle = tokio::spawn(async move {
                temp_ctx.execute(format!("SELECT * FROM table_name WHERE TICKER == {}",temp_tick.clone()).as_str())
            });
            handles.push(handle);
        }
        let mut outputs = Vec::with_capacity(handles.len());
        for task in handles {
            outputs.push(task.await.unwrap().unwrap());
        }
        Some(outputs)
    }

    pub async fn print_df(df: &DataFrame, n: usize) {
        println!("{:?}", df.head(Some(n)));
    }

    pub async fn reit_ticker_vec_split(reit_vec: &Vec<Vec<String>> ) -> Option<Vec<String>>{
        let ticks = reit_vec.into_iter()
                            .map(|x| x[0].split_whitespace()
                                         .nth(0)
                                         .unwrap()
                                         .to_string()).collect();
        Some(ticks)
    }

    pub async fn reitfetch<'i>(url: String) -> Result<(String), ParsError<'i>> {
    	let reit_data: String = reqwest::Client::new().get(url).send().await?.text().await?;
    	Ok(reit_data)
    }

    pub async fn selector_parse<'i>(parse_val: &'i str) -> Result<Selector, ParsError<'i>>{
        let parser = Selector::parse(&parse_val).unwrap();
        Ok(parser)
    }
    pub async fn output_reit_tickers() -> Option<Vec<Vec<String>>>{
        let reit_url_base: &str = "https://www.reit.com/investing/reit-directory?sector=";
        let reit_url_end: &str = "&status=309&country=9";
        let mut array:[i32; 13] = [8308, 8310, 642, 638, 641, 8312, 635, 643, 637, 633, 8309, 8311, 639];
        let mut industries = vec!["Office","Industrial", "Retail", "Lodging/Resorts","Residential",
                                 "Timberlands", "Health Care", "Self-Storage", "Infrastricture",
                                 "Data Center", "Diversified", "Specialty", "Mortgage"].into_iter().map(|x| x.to_string()).collect::<Vec<_>>();
        println!("{:?}",industries);
        let mut nareit: Vec<Vec<Vec<String>>>= vec![];
        let mut nms: Vec<String> = vec![];
        let mut parse_sel_array: Vec<Selector> = vec![];
        for i in 0..array.len(){
            nms.push(format!("{}{}{}",reit_url_base,array[i].to_string(),reit_url_end));
        }
        let sel_array: [&str;10] = ["body", "div.dialog-off-canvas-main-canvas", "div.l-page", "main.l-content",
                                    "div.l-container--narrow", "div.region-content", "div.paragraph--id--972",
                                    "div.field--name-field-views-view", "table.views-table", "tbody"];

        let reit_names = Selector::parse("td.views-field-title").unwrap();
        let ticker_symb = Selector::parse("div.ticker-symbol").unwrap();
        let reit_href = Selector::parse("a").unwrap();
        for j in 0..sel_array.len(){
            parse_sel_array.push(selector_parse(sel_array[j]).await.unwrap());
            println!("{:?}", parse_sel_array[j]);
        }
        loop {
            //if nms.len() == 0 {break;}
            let temp = nms.pop().unwrap();
            let temp_nm = temp.clone();
            let temp_ind = &industries.pop().unwrap();
            println!("{:?}", &temp);
            let reit_data = reitfetch(temp).await.unwrap();
            let document = Html::parse_document(&reit_data);
            let mut count = 0;
            let element = document.select(&parse_sel_array[count]).next().unwrap();
            loop{
                count+=1;
                if count == parse_sel_array.len() { break;}
                let element = element.select(&parse_sel_array[count]).next().unwrap();
                //println!("{:?}",count)
            }
            let mut temp_mat: Vec<Vec<String>> = vec![];
            for tr_data in element.select(&reit_names) {
            temp_mat.push(vec![tr_data.select(&ticker_symb).next().unwrap().inner_html().to_string(), tr_data.select(&reit_href).next().unwrap().inner_html().to_string()]);
            }
            if temp_nm == "https://www.reit.com/investing/reit-directory?sector=638&status=309&country=9"
            {
                return Some(temp_mat);
            }
        }
    }
}
