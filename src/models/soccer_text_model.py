import boto3
from pathlib import Path
from datetime import datetime
import json
import spacy
from typing import List, Dict
from transformers import pipeline
import jmespath

from utils import data_utils, utils
from settings import settings

logger = utils.get_logger(f"{__name__}.log")


class SoccerText:
    def __init__(self, s3_client: boto3.client):
        self.s3_client = s3_client

    def sync_data(self, s3_folder: Path = Path("guardian-match-reports")):
        folder_path = Path("data") / s3_folder
        if folder_path.exists() and folder_path.stat().st_size > 4 * 32:
            folder_update_time = datetime.fromtimestamp(folder_path.stat().st_atime)

        else:
            data_utils.download_dir(
                prefix=str(s3_folder),
                local=folder_path,
                bucket=settings.DATA_S3_BUCKET,
                s3_client=self.s3_client,
            )
            for single_path in folder_path.glob(f"*.jl"):
                data_utils.process_file(single_path)
                logger.info(f"processed {str(single_path)}")
            folder_update_time = datetime.fromtimestamp(folder_path.stat().st_mtime)

        if datetime.now() - folder_update_time > settings.FOLDER_UPDATE_FREQ:
            data_utils.download_dir(
                prefix=str(s3_folder),
                local=folder_path,
                bucket=settings.DATA_S3_BUCKET,
                s3_client=s3_client,
            )
            for single_path in folder_path.glob(f"*.jl"):
                data_utils.process_file(single_path)
                logger.info(f"processed {str(single_path)}")

    def upload_data(self, path: Path):
        response = self.s3_client.upload_file(str(path), settings.DATA_S3_BUCKET, str(path))
        return response

    def get_data(self, path: Path = Path("data/guardian-match-reports")):
        if path.is_file():
            lst_dct = [json.loads(line) for line in open(path, "r").read().split("\n") if line]
        else:
            lst_dct = []
            for file_path in path.glob("*.jl"):
                lst_dct.extend([json.loads(line) for line in open(file_path, "r").read().split("\n") if line])
        return lst_dct


class SoccerTagger(SoccerText):
    def __init__(self, s3_client: boto3.client):
        super().__init__(s3_client)
        self.sync_data(s3_folder=Path("guardian-match-reports"))
        self.articles = self.get_data(path=Path("data/guardian-match-reports"))
        self.spacy = self.load_spacy()
        self.kvstore = data_utils.kvstore("data/processed.db")
        self.sentiment_pipe = pipeline("sentiment-analysis")

    def load_spacy(self):
        nlp = spacy.load("en_core_web_sm")
        return nlp

    def entity_labels(self, doc):
        entities: Dict[str, List[str]] = {"PERSON": []}
        for ent in doc.ents:
            if ent.label_ == "PERSON":
                entities["PERSON"].append(ent.text)
        return entities

    def pos_tag_entities(self, doc, pos_list: list = ["ADV", "ADJ"]):
        def deep_head(token):
            # DEPRECATED
            if token == token.head:
                return token
            else:
                return deep_head(token.head)

        def post_tag_entities_sent(sent, pos_list: list):
            sent_tags: Dict[str, List[str]] = {pos_tag: [] for pos_tag in pos_list}
            sent_tags["ENT"] = []
            ents = {ent.text: range(ent.start_char, ent.end_char) for ent in sent.ents}
            for token in sent:
                if token.pos_ in ["PROPN"]:
                    related_ent = [text for text, idx_range in ents.items() if token.idx in idx_range]
                    related_ent = related_ent[0] if related_ent else token.text
                    sent_tags["ENT"].append(related_ent) if related_ent not in sent_tags["ENT"] else sent_tags["ENT"]
                if token.pos_ in pos_list:
                    sent_tags[token.pos_].append(token.text)
            return sent_tags

        sent_tags = [post_tag_entities_sent(sent, pos_list=pos_list) for sent in doc.sents]
        return sent_tags

    def sentiment(self, doc):
        sentiments = [self.sentiment_pipe(sent.text)[0] for sent in doc.sents]
        sentiments = [{"sentiment": sentiment} for sentiment in sentiments]
        return sentiments

    def forward(self):
        for article in self.articles:
            key = self.kvstore._get_key(article["id"])
            if key in self.kvstore.keys():
                logger.info(f"Ignoring already processed")
            else:
                article = self.forward_pass(article)
                self.save(article, path=Path("data/articles.jl"))
                key = self.kvstore._get_key(article["id"])
                val = self.kvstore._get_val()
                self.kvstore[key] = val
                logger.info(f"Processed article with id {key}")
        self.upload_data(path=Path("data/articles.jl"))

    def forward_pass(self, article: dict):
        try:
            doc = self.spacy(article["text"])
            article["entity_labels"] = self.entity_labels(doc)
            pos_tag_entities = self.pos_tag_entities(doc)
            sentiments = self.sentiment(doc)
            sent_range = [{"start_char": sent.start_char, "end_char": sent.end_char} for sent in doc.sents]
            sentence_info = utils.join_lsts_dct(pos_tag_entities, sentiments, sent_range)
            article["sentence_info"] = sentence_info
        except KeyError as e:
            logger.info(f"Skipped article with id {article['id']} because of missing text")
        return article

    def save(self, article: dict, path: Path = Path("data/articles.jl")):
        with open(path, "a") as jsonl_file:
            json.dump(article, jsonl_file)
            jsonl_file.write("\n")


class SoccerArticles:
    def __init__(self, s3_client: boto3.client):
        super().__init__(s3_client)
        self.sync_data(s3_folder=Path("data/articles.jl"))
        self.articles = self.get_data(path=Path("data/articles.jl"))

    def player_mentions(self):
        res = jmespath.search()
        return res

    # TODO jmespaths or jq to specific articles to ask for, to hand to whatever stuff on top


if __name__ == "__main__":
    s3_client = boto3.client("s3")
    tagger = SoccerTagger(s3_client)
    tagger.forward()
    self = tagger

    article = {
        "scrape_date": "2021-05-25",
        "link": "https://www.theguardian.com/football/2020/feb/23/wolves-norwich-premier-league-match-report",
        "headline": "Diogo Jota double seals easy win for Wolves over Norwich",
        "home_team": "Wolverhampton Wanderers",
        "away_team": "Norwich City",
        "match_date": "2020-02-23",
        "author": "Paul MacInnes",
        "stadium": "Molineux",
        "text": "This was so easy for Wolves it bordered on the surreal. Nuno Esp\u00edrito Santo\u2019s squad have been playing twice a week for as long as Boris Johnson has been the prime minister. Someone should be buying them a break in Mustique but still they plough on and they had too much strength, too much determination (never mind technique and guile), for a Norwich side so desperate for points at the bottom.\n\nTwo goals for Diogo Jota, \n\n, were complemented by a second-half effort from Ra\u00fal Jim\u00e9nez, his 12th league goal of a sparkling campaign. But it could and should have been more and the gulf between the two teams on the day\u2013 only a week after \n\n \u2013 was as great as one is likely to see this season.\n\n\u201cI think it was a good performance,\u201d said Nuno, undercooking it somewhat. \u201cWe played on Thursday and it was the same boys and our game requires a lot of running. I think we run more than the opposite team and that requires a lot of energy.\u201d\n\nThe hosts were fresh from a 4-0 mauling of Espanyol on Thursday night, in all senses of the phrase. It should not be that way: most teams \u2013 and those more celebrated than Wolves \u2013 have struggled with the Europa League calendar. But Wolves have not only avoided mid-season burnout but have grown into the gruelling demands. And 10 of the 11 who beat the Spaniards started again against Norwich, some in effect starting a second season of matches. Conor Coady, for example, was making his 43rd appearance of the campaign, Jim\u00e9nez and Leander Dendoncker their 42nd.\n\nWith 11 games to go they remain in the hunt for the Champions League, five points behind Chelsea in fourth.\n\n\u201cIt\u2019s the way they recover, the way they respect themselves,\u201d said Nuno, asked to explain the secret behind his players\u2019 endurance. \u201cIt\u2019s also the way the staff dedicate. We don\u2019t have days off. We\u2019re always preparing ourselves to compete. We want to compete. The difficulty is to sustain that. Every day is harder than the last.\u201d\n\nNorwich began the game the better side and might have scored had they shown greater intent. Afterwards Daniel Farke accused his attacking players of lacking the necessary \u201cphysicality and body tension\u201d to cut through against well-drilled opponents. It was a kind way of saying that his players, faced by Willy Boly and Romain Sa\u00efss, had bottled it. The Canaries have now gone 536 minutes without a goal from open play and Farke continued: \u201cYou have to give your life with each and every duel and I got the feeling that sometimes we were a bit soft and scared.\u201d\n\nAfter shaking off any stiffness Wolves began to find their bite, led by the irrepressible Jota. He is a renowned runner with the ball but his opening goal in the 19th minute was the work of a poacher. Matt Doherty began the move, getting the better of Jamal Lewis on the right touchline and getting to the box to provide the assist by poking the ball to Jota. Norwich had six men in the box against two but Jota duly spun past Max Aarons and drove a low shot through the legs of Tim Krul.\n\nEleven minutes later Wolves doubled their lead. A short corner on the left came to Jonny whose cross was looped into the box. Dendoncker got underneath it, flicked it to the back post and Sa\u00efss. The enormous defender kept his poise and returned the ball back across goal, where Jota was waiting to tap home. He was one of three Wolves players unmarked at the back post.\n\nThat was the game in a nutshell and, were it not for the keeping of Krul, Norwich would have been further down by half-time, the Dutchman doing superbly to save two R\u00faben Neves free-kicks. In the end it required five minutes of the second half for Wolves to hit their third, a counterattack through Norwich\u2019s outclassed, open rearguard with Jim\u00e9nez burying the rebound after Jota was denied a hat-trick by Krul\u2019s left-hand post.\n\nJota was taken off on the hour to spare his legs, not that he looked as if he needed it. Jim\u00e9nez followed with 20 minutes remaining, to be replaced by the one man rested after Espanyol and the last sight needed by Norwich\u2019s sore eyes, Adama Traor\u00e9. The braided sensation barely touched the ball but by that point he did not need to.",
        "pictures": {
            "src": "https://sb.scorecardresearch.com/p?c1=2&c2=6035250&cv=2.0&cj=1&cs_ucfr=0&comscorekw=Premier+League%2CWolverhampton+Wanderers%2CNorwich+City%2CFootball%2CSport"
        },
        "id": "7959b857bffc1e060aeae18bc9e9ad6e",
        "entity_labels": {
            "PERSON": [
                "Boris Johnson",
                "Diogo Jota",
                "Ra\u00fal Jim\u00e9nez",
                "Nuno",
                "Espanyol",
                "Jim\u00e9nez",
                "Leander Dendoncker",
                "Nuno",
                "Daniel Farke",
                "Willy Boly",
                "Romain Sa\u00efss",
                "Farke",
                "Jota",
                "Matt Doherty",
                "Jamal Lewis",
                "Jota",
                "Max Aarons",
                "Tim Krul",
                "Jonny",
                "Dendoncker",
                "Jota",
                "Jim\u00e9nez",
                "Jota",
                "Jota",
                "Jim\u00e9nez",
                "Espanyol",
                "Adama Traor\u00e9",
            ]
        },
        "sentence_info": [
            [
                {"ADV": ["so"], "ADJ": ["easy"], "ENT": []},
                {"sentiment": {"label": "NEGATIVE", "score": 0.6783755421638489}},
                {"start_char": 0, "end_char": 55},
            ],
            [
                {"ADV": ["as", "long"], "ADJ": ["prime"], "ENT": ["Nuno", "Esp\u00edrito", "Santo", "\u2019s", "Boris Johnson"]},
                {"sentiment": {"label": "POSITIVE", "score": 0.8686425685882568}},
                {"start_char": 56, "end_char": 172},
            ],
            [
                {"ADV": ["still"], "ADJ": [], "ENT": ["Mustique"]},
                {"sentiment": {"label": "NEGATIVE", "score": 0.9987459182739258}},
                {"start_char": 173, "end_char": 247},
            ],
            [
                {"ADV": ["too", "too", "never", "so"], "ADJ": ["much", "much", "desperate"], "ENT": ["Norwich"]},
                {"sentiment": {"label": "NEGATIVE", "score": 0.9917891025543213}},
                {"start_char": 248, "end_char": 394},
            ],
            [
                {"ADV": [], "ADJ": ["second", "12th"], "ENT": ["Diogo Jota", "Ra\u00fal Jim\u00e9nez"]},
                {"sentiment": {"label": "POSITIVE", "score": 0.9997299313545227}},
                {"start_char": 394, "end_char": 532},
            ],
            [
                {"ADV": ["only", "as"], "ADJ": ["more", "great", "likely"], "ENT": []},
                {"sentiment": {"label": "POSITIVE", "score": 0.9968041777610779}},
                {"start_char": 533, "end_char": 691},
            ],
            [
                {"ADV": ["somewhat"], "ADJ": ["good"], "ENT": ["Nuno"]},
                {"sentiment": {"label": "POSITIVE", "score": 0.9904531836509705}},
                {"start_char": 691, "end_char": 766},
            ],
            [
                {"ADV": [], "ADJ": ["same"], "ENT": ["Thursday"]},
                {"sentiment": {"label": "NEGATIVE", "score": 0.9961199760437012}},
                {"start_char": 767, "end_char": 854},
            ],
            [
                {"ADV": [], "ADJ": ["more", "opposite"], "ENT": []},
                {"sentiment": {"label": "NEGATIVE", "score": 0.9640998244285583}},
                {"start_char": 855, "end_char": 933},
            ],
            [
                {"ADV": [], "ADJ": ["fresh"], "ENT": ["Espanyol", "Thursday"]},
                {"sentiment": {"label": "POSITIVE", "score": 0.9138003587722778}},
                {"start_char": 933, "end_char": 1034},
            ],
            [
                {"ADV": ["more"], "ADJ": ["most", "celebrated"], "ENT": ["the Europa League"]},
                {"sentiment": {"label": "NEGATIVE", "score": 0.9962999820709229}},
                {"start_char": 1035, "end_char": 1162},
            ],
            [
                {"ADV": ["only"], "ADJ": ["mid", "gruelling"], "ENT": []},
                {"sentiment": {"label": "NEGATIVE", "score": 0.6025155186653137}},
                {"start_char": 1163, "end_char": 1257},
            ],
            [
                {"ADV": ["again"], "ADJ": ["second"], "ENT": ["Spaniards", "Norwich"]},
                {"sentiment": {"label": "POSITIVE", "score": 0.9856644868850708}},
                {"start_char": 1258, "end_char": 1380},
            ],
            [
                {"ADV": [], "ADJ": ["43rd"], "ENT": ["Coady", "Jim\u00e9nez", "Leander Dendoncker"]},
                {"sentiment": {"label": "POSITIVE", "score": 0.9955624341964722}},
                {"start_char": 1381, "end_char": 1497},
            ],
            [
                {"ADV": [], "ADJ": [], "ENT": []},
                {"sentiment": {"label": "POSITIVE", "score": 0.7481210827827454}},
                {"start_char": 1497, "end_char": 1499},
            ],
            [
                {"ADV": [], "ADJ": [], "ENT": ["the Champions League", "Chelsea"]},
                {"sentiment": {"label": "POSITIVE", "score": 0.9989736676216125}},
                {"start_char": 1499, "end_char": 1606},
            ],
            [
                {"ADV": [], "ADJ": [], "ENT": ["Nuno"]},
                {"sentiment": {"label": "POSITIVE", "score": 0.9932759404182434}},
                {"start_char": 1606, "end_char": 1739},
            ],
            [
                {"ADV": ["also"], "ADJ": [], "ENT": []},
                {"sentiment": {"label": "NEGATIVE", "score": 0.6992451548576355}},
                {"start_char": 1740, "end_char": 1778},
            ],
            [
                {"ADV": [], "ADJ": [], "ENT": []},
                {"sentiment": {"label": "POSITIVE", "score": 0.8699759840965271}},
                {"start_char": 1779, "end_char": 1802},
            ],
            [
                {"ADV": ["always"], "ADJ": [], "ENT": []},
                {"sentiment": {"label": "POSITIVE", "score": 0.9990865588188171}},
                {"start_char": 1803, "end_char": 1847},
            ],
            [
                {"ADV": [], "ADJ": [], "ENT": []},
                {"sentiment": {"label": "POSITIVE", "score": 0.9994788765907288}},
                {"start_char": 1848, "end_char": 1867},
            ],
            [
                {"ADV": [], "ADJ": [], "ENT": []},
                {"sentiment": {"label": "NEGATIVE", "score": 0.9963915348052979}},
                {"start_char": 1868, "end_char": 1902},
            ],
            [
                {"ADV": [], "ADJ": ["harder", "last"], "ENT": []},
                {"sentiment": {"label": "NEGATIVE", "score": 0.9958826899528503}},
                {"start_char": 1903, "end_char": 1938},
            ],
            [
                {"ADV": [], "ADJ": ["better", "greater"], "ENT": ["Norwich"]},
                {"sentiment": {"label": "POSITIVE", "score": 0.8864847421646118}},
                {"start_char": 1938, "end_char": 2031},
            ],
            [
                {"ADV": ["Afterwards", "well"], "ADJ": ["necessary"], "ENT": ["Daniel Farke"]},
                {"sentiment": {"label": "NEGATIVE", "score": 0.9985236525535583}},
                {"start_char": 2032, "end_char": 2188},
            ],
            [
                {"ADV": [], "ADJ": ["kind"], "ENT": ["Willy Boly", "Romain Sa\u00efss"]},
                {"sentiment": {"label": "POSITIVE", "score": 0.8749172687530518}},
                {"start_char": 2189, "end_char": 2288},
            ],
            [
                {"ADV": ["now", "sometimes"], "ADJ": ["open", "soft", "scared"], "ENT": ["Farke"]},
                {"sentiment": {"label": "POSITIVE", "score": 0.9834980368614197}},
                {"start_char": 2289, "end_char": 2500},
            ],
            [
                {"ADV": [], "ADJ": ["irrepressible"], "ENT": ["Jota"]},
                {"sentiment": {"label": "NEGATIVE", "score": 0.9240871071815491}},
                {"start_char": 2500, "end_char": 2597},
            ],
            [
                {"ADV": [], "ADJ": ["renowned", "19th"], "ENT": []},
                {"sentiment": {"label": "POSITIVE", "score": 0.9617944359779358}},
                {"start_char": 2598, "end_char": 2702},
            ],
            [
                {"ADV": [], "ADJ": ["better", "right"], "ENT": ["Matt Doherty", "Jamal Lewis", "Jota"]},
                {"sentiment": {"label": "POSITIVE", "score": 0.9955102205276489}},
                {"start_char": 2703, "end_char": 2861},
            ],
            [
                {"ADV": ["duly"], "ADJ": ["low"], "ENT": ["Norwich", "Jota", "Max Aarons", "Tim Krul"]},
                {"sentiment": {"label": "POSITIVE", "score": 0.9973445534706116}},
                {"start_char": 2862, "end_char": 2990},
            ],
            [
                {"ADV": [], "ADJ": [], "ENT": []},
                {"sentiment": {"label": "POSITIVE", "score": 0.7481210827827454}},
                {"start_char": 2990, "end_char": 2992},
            ],
            [
                {"ADV": ["later"], "ADJ": [], "ENT": []},
                {"sentiment": {"label": "POSITIVE", "score": 0.984054684638977}},
                {"start_char": 2992, "end_char": 3039},
            ],
            [
                {"ADV": [], "ADJ": ["short"], "ENT": ["Jonny"]},
                {"sentiment": {"label": "NEGATIVE", "score": 0.9751298427581787}},
                {"start_char": 3040, "end_char": 3117},
            ],
            [
                {"ADV": [], "ADJ": ["back"], "ENT": ["Dendoncker", "Sa\u00efss"]},
                {"sentiment": {"label": "NEGATIVE", "score": 0.9775878190994263}},
                {"start_char": 3118, "end_char": 3186},
            ],
            [
                {"ADV": ["back", "where", "home"], "ADJ": ["enormous"], "ENT": ["Jota"]},
                {"sentiment": {"label": "POSITIVE", "score": 0.9902887344360352}},
                {"start_char": 3187, "end_char": 3299},
            ],
            [
                {"ADV": [], "ADJ": ["back"], "ENT": []},
                {"sentiment": {"label": "NEGATIVE", "score": 0.9718198180198669}},
                {"start_char": 3300, "end_char": 3361},
            ],
            [
                {"ADV": [], "ADJ": [], "ENT": []},
                {"sentiment": {"label": "POSITIVE", "score": 0.7481210827827454}},
                {"start_char": 3361, "end_char": 3363},
            ],
            [
                {
                    "ADV": ["further", "down", "superbly"],
                    "ADJ": ["half", "free"],
                    "ENT": ["Krul", "Norwich", "Dutchman", "R\u00faben", "Neves"],
                },
                {"sentiment": {"label": "NEGATIVE", "score": 0.9912363886833191}},
                {"start_char": 3363, "end_char": 3551},
            ],
            [
                {
                    "ADV": [],
                    "ADJ": ["second", "third", "outclassed", "open", "left"],
                    "ENT": ["Norwich\u2019s", "Jim\u00e9nez", "Jota", "Krul\u2019s"],
                },
                {"sentiment": {"label": "POSITIVE", "score": 0.6771166920661926}},
                {"start_char": 3552, "end_char": 3793},
            ],
            [
                {"ADV": [], "ADJ": [], "ENT": ["Jota"]},
                {"sentiment": {"label": "NEGATIVE", "score": 0.9973220229148865}},
                {"start_char": 3793, "end_char": 3881},
            ],
            [
                {"ADV": [], "ADJ": ["last", "sore"], "ENT": ["Jim\u00e9nez", "Espanyol", "Norwich\u2019s", "Adama Traor\u00e9"]},
                {"sentiment": {"label": "NEGATIVE", "score": 0.9947760701179504}},
                {"start_char": 3882, "end_char": 4041},
            ],
            [
                {"ADV": ["barely"], "ADJ": ["braided"], "ENT": []},
                {"sentiment": {"label": "POSITIVE", "score": 0.9844018816947937}},
                {"start_char": 4042, "end_char": 4125},
            ],
        ],
    }
