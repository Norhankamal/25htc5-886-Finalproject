import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import boto3

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------
NETID        = '25htc5'
BUCKET_NAME  = '25htc5-project-data'          # FIX: correct bucket name (no "group31")
BUCKET       = f's3://{BUCKET_NAME}'

# Input paths
RAW_REVIEWS  = f'{BUCKET}/raw/reviews/part_*.jsonl'
RAW_META     = f'{BUCKET}/raw/meta/part_*.jsonl'

# Output paths
OUT_BASE     = f'{BUCKET}/processed'
EDA_S3       = f'{BUCKET}/eda'

# Quality filters
MIN_REVIEW_WORDS = 20
MIN_REVIEWS_PROD = 5

# FIX: reduced from 500,000 to 200,000 to complete within EMR cluster lifetime
MAX_REVIEWS = 200_000

# Reproducibility
RANDOM_SEED      = 42
SAMPLE_PREVIEW_N = 100

# -----------------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# SPARK SESSION
# FIX: disable dynamic allocation and set explicit executor resources
# to prevent the job from requesting 100 executors on a 2-node cluster
# -----------------------------------------------------------------------------
def create_spark_session():
    log.info('Creating Spark session...')
    spark = (
        SparkSession.builder
        .appName(f'{NETID}-beauty-preprocessing')
        # FIX: disable dynamic allocation — it was requesting 100 executors
        # which caused indefinite PENDING on a small cluster
        .config('spark.dynamicAllocation.enabled',               'false')
        .config('spark.executor.instances',                      '2')
        .config('spark.executor.cores',                          '2')
        .config('spark.executor.memory',                         '4g')
        .config('spark.driver.memory',                           '2g')
        # Keep adaptive query execution for shuffle optimization
        .config('spark.sql.adaptive.enabled',                    'true')
        .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
        .config('spark.sql.shuffle.partitions',                  '100')
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        .config('spark.sql.parquet.compression.codec',           'snappy')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('WARN')
    log.info(f'Spark version : {spark.version}')
    log.info(f'Executor count: 2  |  Cores per executor: 2  |  Memory: 4g')
    return spark

# -----------------------------------------------------------------------------
# SCHEMAS
# -----------------------------------------------------------------------------
REVIEW_SCHEMA = StructType([
    StructField('rating',            FloatType(),   True),
    StructField('title',             StringType(),  True),
    StructField('text',              StringType(),  True),
    StructField('images',            StringType(),  True),
    StructField('asin',              StringType(),  True),
    StructField('parent_asin',       StringType(),  True),
    StructField('user_id',           StringType(),  True),
    StructField('timestamp',         LongType(),    True),
    StructField('helpful_vote',      IntegerType(), True),
    StructField('verified_purchase', BooleanType(), True),
])

META_SCHEMA = StructType([
    StructField('main_category',   StringType(),  True),
    StructField('title',           StringType(),  True),
    StructField('average_rating',  FloatType(),   True),
    StructField('rating_number',   IntegerType(), True),
    StructField('features',        StringType(),  True),
    StructField('description',     StringType(),  True),
    StructField('price',           StringType(),  True),
    StructField('images',          StringType(),  True),
    StructField('videos',          StringType(),  True),
    StructField('store',           StringType(),  True),
    StructField('categories',      StringType(),  True),
    StructField('details',         StringType(),  True),
    StructField('parent_asin',     StringType(),  True),
    StructField('bought_together', StringType(),  True),
])

# -----------------------------------------------------------------------------
# STAGE 1 — LOAD DATA
# -----------------------------------------------------------------------------
def load_data(spark):
    log.info('=' * 60)
    log.info('STAGE 1 — Loading raw data from S3')
    log.info(f'  Reviews path : {RAW_REVIEWS}')
    log.info(f'  Meta path    : {RAW_META}')
    log.info('=' * 60)

    reviews_raw = spark.read.schema(REVIEW_SCHEMA).json(RAW_REVIEWS)

    if MAX_REVIEWS is not None:
        reviews_raw = reviews_raw.limit(MAX_REVIEWS)
        log.info(f'Sample cap applied: {MAX_REVIEWS:,} reviews')

    review_count = reviews_raw.count()
    log.info(f'Reviews loaded: {review_count:,}')

    try:
        log.info(f'Loading metadata...')
        meta_raw = spark.read.schema(META_SCHEMA).json(RAW_META)
        meta_count = meta_raw.count()
        log.info(f'Metadata loaded: {meta_count:,}')
    except Exception as e:
        log.warning(f'Metadata not found or failed to load: {e}')
        log.warning('Continuing without metadata.')
        meta_raw = None

    return reviews_raw, meta_raw

# -----------------------------------------------------------------------------
# STAGE 2 — CLEAN REVIEWS
# -----------------------------------------------------------------------------
@F.udf(StringType())
def clean_text_udf(text):
    if not text:
        return None
    text = html.unescape(text)
    # FIX A: Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # FIX A: Remove [[VIDEOID:...]] and similar bracket tokens that pollute model output
    text = re.sub(r'\[\[[A-Z]+:[^\]]*\]\]', ' ', text)
    # Also remove bare [VIDEOID:...] variants
    text = re.sub(r'\[[A-Z]+:[^\]]*\]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text if text else None

@F.udf(IntegerType())
def word_count_udf(text):
    if not text:
        return 0
    return len(text.split())

@F.udf(FloatType())
def parse_price_udf(price_str):
    if not price_str:
        return None
    try:
        cleaned = re.sub(r'[^\d.]', '', str(price_str))
        val = float(cleaned)
        return val if val > 0 else None
    except Exception:
        return None


def clean_reviews(reviews_raw):
    log.info('=' * 60)
    log.info('STAGE 2 — Cleaning reviews')
    log.info('=' * 60)

    reviews = reviews_raw.withColumn(
        'product_id',
        F.when(
            F.col('parent_asin').isNotNull() & (F.col('parent_asin') != ''),
            F.col('parent_asin')
        ).otherwise(F.col('asin'))
    )

    total = reviews.count()
    log.info(f'Before cleaning: {total:,}')

    reviews = reviews.withColumn('clean_text', clean_text_udf(F.col('text')))
    reviews = reviews.filter(
        F.col('clean_text').isNotNull() &
        F.col('product_id').isNotNull() &
        (F.col('product_id') != '')
    )
    after_nulls = reviews.count()
    log.info(f'After null drop: {after_nulls:,} (removed {total - after_nulls:,})')

    reviews = reviews.withColumn('word_count', word_count_udf(F.col('clean_text')))
    reviews = reviews.filter(F.col('word_count') >= MIN_REVIEW_WORDS)
    after_length = reviews.count()
    log.info(f'After length filter (>={MIN_REVIEW_WORDS} words): {after_length:,}')

    reviews = reviews.dropDuplicates(['product_id', 'clean_text'])
    after_dedup = reviews.count()
    log.info(f'After dedup: {after_dedup:,} (removed {after_length - after_dedup:,})')

    reviews = reviews.fillna({'helpful_vote': 0, 'verified_purchase': False, 'rating': 3.0})

    log.info(f'Reviews kept: {after_dedup:,}')
    return reviews

# -----------------------------------------------------------------------------
# STAGE 3 — METADATA JOIN
# -----------------------------------------------------------------------------
@F.udf(BooleanType())
def is_beauty_udf(title, categories):
    beauty_kws = {
        'beauty', 'skin', 'hair', 'makeup', 'cosmetic', 'serum',
        'moisturizer', 'shampoo', 'conditioner', 'lotion', 'cream',
        'cleanser', 'foundation', 'lipstick', 'mascara', 'blush',
        'eyeshadow', 'nail', 'perfume', 'fragrance', 'sunscreen',
        'spf', 'toner', 'exfoliant', 'scrub', 'mask', 'deodorant',
        'body wash', 'soap', 'razor', 'shaving', 'lash', 'brow',
        'concealer', 'primer', 'highlighter', 'contour', 'bronzer',
        'gloss', 'liner', 'pencil', 'face wash', 'eye cream',
    }
    combined = (title or '').lower() + ' ' + (categories or '').lower()
    return any(kw in combined for kw in beauty_kws)


def clean_and_join(spark, reviews, meta_raw):
    log.info('=' * 60)
    log.info('STAGE 3 — Metadata join')
    log.info('=' * 60)

    if meta_raw is None:
        log.warning('No metadata — adding placeholder columns.')
        reviews = (
            reviews
            .withColumn('product_title', F.lit('Unknown Product'))
            .withColumn('price_raw',     F.lit(None).cast(StringType()))
            .withColumn('price_num',     F.lit(None).cast(FloatType()))
            .withColumn('budget_tier',   F.lit('unknown'))
            .withColumn('store',         F.lit(None).cast(StringType()))
        )
        return reviews

    meta = (
        meta_raw
        .select(
            F.col('parent_asin').alias('product_id'),
            F.col('title').alias('product_title'),
            F.col('price').alias('price_raw'),
            F.col('store'),
            F.col('categories').alias('categories_raw'),
        )
        .filter(
            F.col('product_id').isNotNull() &
            F.col('product_title').isNotNull() &
            (F.col('product_title') != '')
        )
        .withColumn('price_num', parse_price_udf(F.col('price_raw')))
        .withColumn(
            'budget_tier',
            F.when(F.col('price_num') < 15,  'low')
             .when(F.col('price_num') <= 30, 'mid')
             .when(F.col('price_num') > 30,  'high')
             .otherwise('unknown')
        )
        .filter(is_beauty_udf(F.col('product_title'), F.col('categories_raw')))
        .dropDuplicates(['product_id'])
    )

    meta_count = meta.count()
    log.info(f'Clean beauty metadata: {meta_count:,}')

    joined = reviews.join(meta, on='product_id', how='inner')
    joined_count = joined.count()
    log.info(f'Records after join: {joined_count:,}')
    return joined

# -----------------------------------------------------------------------------
# STAGE 4 — AGGREGATE BY PRODUCT
# -----------------------------------------------------------------------------
SKIN_PATTERNS = {
    'oily'       : ['oily skin', 'greasy skin', 'shiny skin', 'oily t-zone'],
    'dry'        : ['dry skin', 'dry face', 'flaky skin', 'dehydrated skin'],
    'sensitive'  : ['sensitive skin', 'easily irritated', 'reactive skin'],
    'combination': ['combination skin', 'mixed skin'],
    'acne'       : ['acne', 'breakouts', 'pimples', 'blemishes', 'acne-prone'],
    'aging'      : ['anti-aging', 'wrinkles', 'fine lines', 'mature skin'],
}

@F.udf(StringType())
def detect_skin_type_udf(text):
    """
    FIX B: Instead of returning only the FIRST skin type found,
    return all matched skin types as a comma-separated string.
    This way collect_list captures multiple skin signals per review,
    making the Counter vote in generate_pairs_udf more accurate.
    """
    if not text:
        return None
    text_lower = text.lower()
    matched = []
    for skin_type, patterns in SKIN_PATTERNS.items():
        if any(p in text_lower for p in patterns):
            matched.append(skin_type)
    return ','.join(matched) if matched else None

@F.udf(StringType())
def detect_concern_udf(text):
    concern_patterns = {
        'hydration'     : ['hydrating', 'moisturizing', 'moisture', 'dry skin'],
        'brightening'   : ['brightening', 'glow', 'radiant', 'luminous'],
        'anti_aging'    : ['anti-aging', 'wrinkles', 'fine lines', 'firming'],
        'spf'           : ['spf', 'sunscreen', 'sun protection'],
        'fragrance_free': ['fragrance-free', 'unscented'],
        'natural'       : ['natural', 'organic', 'vegan', 'cruelty-free'],
    }
    if not text:
        return None
    text_lower = text.lower()
    for concern, patterns in concern_patterns.items():
        if any(p in text_lower for p in patterns):
            return concern
    return None


def aggregate_products(spark, joined):
    log.info('=' * 60)
    log.info('STAGE 4 — Aggregating by product')
    log.info('=' * 60)

    with_skin = (
        joined
        .withColumn('skin_type', detect_skin_type_udf(F.col('clean_text')))
        .withColumn('concern',   detect_concern_udf(F.col('clean_text')))
    )

    product_agg = (
        with_skin
        .groupBy('product_id')
        .agg(
            F.first('product_title',  ignorenulls=True).alias('title'),
            F.first('price_raw',      ignorenulls=True).alias('price_raw'),
            F.first('budget_tier',    ignorenulls=True).alias('budget_tier'),
            F.avg('rating').alias('avg_rating'),
            F.count('*').alias('review_count'),
            F.sum(F.when(F.col('verified_purchase'), 1).otherwise(0)).alias('verified_count'),
            F.sum(F.when(F.col('rating') >= 4.0, 1).otherwise(0)).alias('pos_count'),
            F.sum(F.when(F.col('rating') <= 2.0, 1).otherwise(0)).alias('neg_count'),
            # FIX C: collect (helpful_vote, clean_text) structs so we can sort by helpfulness
            # Serialized as JSON string array to avoid complex schema in UDF
            F.collect_list(
                F.when(F.col('rating') >= 4.0,
                       F.to_json(F.struct(
                           F.col('helpful_vote').alias('helpful_vote'),
                           F.col('clean_text').alias('clean_text')
                       )))
            ).alias('pos_reviews_raw'),
            F.collect_list(
                F.when(F.col('rating') <= 2.0,
                       F.to_json(F.struct(
                           F.col('helpful_vote').alias('helpful_vote'),
                           F.col('clean_text').alias('clean_text')
                       )))
            ).alias('neg_reviews_raw'),
            # FIX B: skin_type may now be comma-separated multi-values — explode them via split
            F.collect_list('skin_type').alias('skin_type_votes'),
            F.collect_list('concern').alias('concern_votes'),
        )
        .filter(F.col('review_count') >= MIN_REVIEWS_PROD)
        .withColumn(
            'verified_pct',
            (F.col('verified_count') / F.col('review_count') * 100).cast(IntegerType())
        )
        .withColumn(
            'pos_pct',
            (F.col('pos_count') / F.col('review_count') * 100).cast(IntegerType())
        )
    )

    prod_count = product_agg.count()
    log.info(f'Products with >= {MIN_REVIEWS_PROD} reviews: {prod_count:,}')
    return product_agg

# -----------------------------------------------------------------------------
# STAGE 5 — GENERATE Q&A PAIRS
# -----------------------------------------------------------------------------
SYSTEM_PROMPT = (
    "You are a knowledgeable beauty and personal care product assistant. "
    "You provide accurate, personalized product recommendations based on "
    "skin type, concerns, and verified customer reviews."
)

def _det_hash(s, n):
    return int(hashlib.md5(s.encode()).hexdigest(), 16) % n

def _truncate(text, max_words):
    if not text:
        return ''
    words = text.split()
    return ' '.join(words[:max_words]) + ('...' if len(words) > max_words else '')


@F.udf(StringType())
def generate_pairs_udf(product_id, title, price_raw, budget_tier,
                        avg_rating, review_count, verified_pct,
                        pos_count, neg_count,
                        pos_reviews_raw, neg_reviews_raw,
                        skin_type_votes, concern_votes):
    try:
        if not product_id or not title:
            return json.dumps([])

        rating     = round(float(avg_rating or 3.0), 1)
        count      = int(review_count or 0)
        ver_pct    = int(verified_pct or 0)
        pos_pct    = round(int(pos_count or 0) / max(count, 1) * 100)
        budget_label = str(budget_tier or 'unknown')
        short_title  = str(title).strip()[:60]

        price_str = ''
        if price_raw and str(price_raw).strip() not in ('', 'None', 'nan'):
            price_str = f' (priced at {price_raw})'

        pos_reviews_raw_in = pos_reviews_raw or []
        neg_reviews_raw_in = neg_reviews_raw or []

        # FIX C: pos/neg reviews are now JSON strings: '{"helpful_vote": N, "clean_text": "..."}'
        # Sort by helpful_vote descending — most helpful review first (not longest)
        def _extract_reviews(raw_list):
            """Parse JSON review entries, filter short texts, sort by helpfulness."""
            results = []
            for item in raw_list:
                if item is None:
                    continue
                try:
                    obj = json.loads(item)
                    text = obj.get('clean_text', '')
                    votes = int(obj.get('helpful_vote') or 0)
                except Exception:
                    # Fallback: treat as plain string
                    text = str(item)
                    votes = 0
                if text and len(text.split()) >= 10:
                    results.append((votes, text))
            # Sort by helpful_vote descending
            results.sort(key=lambda x: x[0], reverse=True)
            return [text for _, text in results]

        pos_reviews = _extract_reviews(pos_reviews_raw_in)
        neg_reviews = _extract_reviews(neg_reviews_raw_in)

        # FIX D: Filter negative/complaint words from best_positive reviews
        # We only want genuinely positive snippets in the recommendation output
        NEGATIVE_WORDS = {
            'terrible', 'awful', 'horrible', 'disgusting', 'worst', 'hate',
            'broke out', 'breakout', 'rash', 'burned', 'burning', 'irritated',
            'allergic', 'reaction', 'returned', 'refund', 'waste', 'scam',
            'disappointed', 'misleading', 'fake', 'counterfeit', 'smell bad',
            'smells bad', 'smells awful', 'made me', 'made my skin',
        }

        def _is_clean_positive(text):
            t = text.lower()
            return not any(neg in t for neg in NEGATIVE_WORDS)

        clean_pos = [r for r in pos_reviews if _is_clean_positive(r)]
        # Fall back to unfiltered if nothing passes (edge case)
        best_pos = (clean_pos if clean_pos else pos_reviews)[:3]
        best_neg = neg_reviews[:3]
        top_pos  = best_pos[0] if best_pos else None
        top_neg  = best_neg[0] if best_neg else None

        # FIX B: skin_type_votes may contain comma-separated multi-values per review
        # Flatten them all into one list before voting
        raw_skin_votes = [v for v in (skin_type_votes or []) if v]
        flat_skin_votes = []
        for v in raw_skin_votes:
            flat_skin_votes.extend(v.split(','))
        flat_skin_votes = [s.strip() for s in flat_skin_votes if s.strip()]

        # FIX B: require at least 2 reviews mentioning a skin type before trusting it
        skin_type = None
        if flat_skin_votes:
            counter = Counter(flat_skin_votes)
            for candidate, freq in counter.most_common():
                if freq >= 2:
                    skin_type = candidate
                    break
            # If nothing has >= 2 votes, fall back to most common anyway
            if skin_type is None:
                skin_type = counter.most_common(1)[0][0]

        concern_votes_clean = [v for v in (concern_votes or []) if v]
        concern = None
        if concern_votes_clean:
            concern = Counter(concern_votes_clean).most_common(1)[0][0]

        skin_label = (skin_type or 'all').replace('_', ' ') + ' skin'
        problem    = (concern or 'general skincare').replace('_', ' ')

        if rating >= 4.0:
            answer_type = 'positive'
            reason  = f'customers consistently report effective results for {problem}'
            caution = 'results may vary based on individual skin chemistry'
        elif rating >= 3.0:
            answer_type = 'mixed'
            reason  = f'it shows moderate effectiveness for {problem}'
            caution = 'this product has mixed reviews — patch test recommended'
        else:
            answer_type = 'negative'
            reason  = f'many customers report limited effectiveness for {problem}'
            caution = 'consider consulting a dermatologist before purchase'

        pairs = []

        # TYPE 1 — USER-AWARE RECOMMENDATION
        rec_questions = [
            f'Can you recommend {short_title} for {skin_label} dealing with {problem}?',
            f'Is {short_title} good for {skin_label} with {problem}?',
            f'Should I buy {short_title} if I have {skin_label} and {problem}?',
            f'What do customers with {skin_label} say about {short_title} for {problem}?',
        ]
        instruction = rec_questions[_det_hash(product_id + 'rec', len(rec_questions))]

        if answer_type == 'positive':
            output = (
                f'{short_title} is a strong choice for {skin_label} dealing with {problem}. '
                f'Specifically, {reason}. '
            )
            if top_pos:
                output += f'Customers confirm this: "{_truncate(top_pos, 35)}" '
            output += (
                f'It is rated {rating}/5 across {count} reviews{price_str}. '
                f'One thing to keep in mind: {caution}.'
            )
        elif answer_type == 'mixed':
            output = (
                f'{short_title} may work for {skin_label} with {problem}, '
                f'but results are mixed (rated {rating}/5 across {count} reviews). '
            )
            if top_pos:
                output += f'Some customers say: "{_truncate(top_pos, 30)}" '
            if top_neg:
                output += f'However, others report: "{_truncate(top_neg, 30)}" '
            output += f'Note that {caution}.'
        else:
            output = (
                f'{short_title} may not be the best fit for {skin_label} with {problem} '
                f'(rated {rating}/5 across {count} reviews). '
            )
            if top_neg:
                output += f'Customers report: "{_truncate(top_neg, 35)}" '
            output += f'Consider looking for alternatives. {caution}.'

        pairs.append({
            'instruction': instruction, 'input': '', 'output': output.strip(),
            'type': 'user_aware_recommendation', 'skin_type': skin_type,
            'problem': problem, 'budget': budget_label, 'product_id': product_id,
        })

        # TYPE 2 — REASONED SENTIMENT SUMMARY
        if best_pos or best_neg:
            summary_questions = [
                f'What do people with {skin_label} say about {short_title}?',
                f'Is {short_title} actually good for {skin_label} and {problem}?',
                f'Give me an honest summary of {short_title} for {skin_label}.',
            ]
            instruction = summary_questions[_det_hash(product_id + 'sum', len(summary_questions))]
            lines = [
                f'Here is an honest breakdown of {short_title} specifically for {skin_label} with {problem}:',
                '',
                f'Rating: {rating}/5 across {count} customer reviews ({ver_pct}% verified purchases).',
                '',
            ]
            if best_pos:
                lines += [
                    'What works well:',
                    f'- {reason.capitalize()}.',
                    f'- Customers say: "{_truncate(best_pos[0], 35)}"',
                    '',
                ]
            if best_neg:
                lines += [
                    'What to be aware of:',
                    f'- {caution.capitalize()}.',
                    f'- Some customers report: "{_truncate(best_neg[0], 35)}"',
                    '',
                ]
            verdict = (
                'Recommended' if rating >= 4.0
                else ('Worth trying cautiously' if rating >= 3.0
                      else 'Consider alternatives')
            )
            lines.append(
                f'Bottom line: {pos_pct}% of reviewers had positive experiences. '
                f'{verdict} for {skin_label} dealing with {problem}.'
            )
            pairs.append({
                'instruction': instruction, 'input': '',
                'output': '\n'.join(lines).strip(),
                'type': 'reasoned_sentiment_summary', 'skin_type': skin_type,
                'problem': problem, 'budget': budget_label, 'product_id': product_id,
            })

        # TYPE 3 — SPECIFIC REASONED Q&A
        specific_questions = [
            f'Will {short_title} help with my {problem} if I have {skin_label}?',
            f'Is {short_title} safe for {skin_label} prone to {problem}?',
            f'How does {short_title} perform for {skin_label} with {problem}?',
        ]
        instruction = specific_questions[_det_hash(product_id + 'spec', len(specific_questions))]

        if answer_type == 'positive':
            output = (
                f'Yes, {short_title} is well-suited for {skin_label} with {problem}. '
                f'{reason.capitalize()}, which directly addresses your concern. '
            )
            if top_pos:
                output += f'A customer with similar concerns noted: "{_truncate(top_pos, 40)}" '
        elif answer_type == 'mixed':
            output = (
                f'{short_title} may help with {problem} on {skin_label}, '
                f'but results vary (rated {rating}/5). '
                f'{reason.capitalize()}, though not consistently. '
            )
            if top_neg:
                output += f'Some users note: "{_truncate(top_neg, 30)}" '
        else:
            output = (
                f'{short_title} may not be ideal for {skin_label} with {problem} '
                f'(rated {rating}/5). '
            )
            if top_neg:
                output += f'Customers report: "{_truncate(top_neg, 35)}" '
        output += f'Keep in mind: {caution}.'

        pairs.append({
            'instruction': instruction, 'input': '', 'output': output.strip(),
            'type': 'specific_reasoned_qa', 'skin_type': skin_type,
            'problem': problem, 'budget': budget_label, 'product_id': product_id,
        })

        return json.dumps(pairs)

    except Exception:
        return json.dumps([])


def generate_qa_pairs(spark, product_agg):
    log.info('=' * 60)
    log.info('STAGE 5 — Generating Q&A pairs')
    log.info('=' * 60)

    with_pairs = product_agg.withColumn(
        'pairs_json',
        generate_pairs_udf(
            F.col('product_id'), F.col('title'), F.col('price_raw'), F.col('budget_tier'),
            F.col('avg_rating'), F.col('review_count'), F.col('verified_pct'),
            F.col('pos_count'), F.col('neg_count'),
            F.col('pos_reviews_raw'), F.col('neg_reviews_raw'),
            F.col('skin_type_votes'), F.col('concern_votes'),
        )
    )

    @F.udf(ArrayType(StringType()))
    def parse_pairs_udf(json_str):
        try:
            return [json.dumps(p) for p in json.loads(json_str or '[]')]
        except Exception:
            return []

    @F.udf(StructType([
        StructField('instruction', StringType(), True),
        StructField('input',       StringType(), True),
        StructField('output',      StringType(), True),
        StructField('type',        StringType(), True),
        StructField('skin_type',   StringType(), True),
        StructField('problem',     StringType(), True),
        StructField('budget',      StringType(), True),
        StructField('product_id',  StringType(), True),
    ]))
    def parse_pair_udf(pair_str):
        try:
            p = json.loads(pair_str)
            return (
                p.get('instruction', ''), p.get('input', ''), p.get('output', ''),
                p.get('type', ''), p.get('skin_type', ''), p.get('problem', ''),
                p.get('budget', ''), p.get('product_id', ''),
            )
        except Exception:
            return None

    pairs_df = (
        with_pairs
        .withColumn('pair_str', F.explode(parse_pairs_udf(F.col('pairs_json'))))
        .withColumn('pair', parse_pair_udf(F.col('pair_str')))
        .select(
            F.col('pair.instruction').alias('instruction'),
            F.col('pair.input').alias('input'),
            F.col('pair.output').alias('output'),
            F.col('pair.type').alias('type'),
            F.col('pair.skin_type').alias('skin_type'),
            F.col('pair.problem').alias('problem'),
            F.col('pair.budget').alias('budget'),
            F.col('pair.product_id').alias('product_id'),
        )
        .filter(
            F.col('instruction').isNotNull() & F.col('output').isNotNull() &
            (F.col('instruction') != '') & (F.col('output') != '')
        )
    )

    # Add Llama 3.2 chat format column for fine-tuning
    BOS          = '<|begin_of_text|>'
    START_HEADER = '<|start_header_id|>'
    END_HEADER   = '<|end_header_id|>'
    EOT          = '<|eot_id|>'

    pairs_df = pairs_df.withColumn(
        'text',
        F.concat(
            F.lit(f'{BOS}{START_HEADER}system{END_HEADER}\n{SYSTEM_PROMPT}{EOT}\n'),
            F.lit(f'{START_HEADER}user{END_HEADER}\n'),
            F.col('instruction'),
            F.lit(f'{EOT}\n{START_HEADER}assistant{END_HEADER}\n'),
            F.col('output'),
            F.lit(EOT),
        )
    )

    total_pairs = pairs_df.count()
    log.info(f'Total Q&A pairs generated: {total_pairs:,}')
    for row in pairs_df.groupBy('type').count().collect():
        log.info(f'  {row["type"]}: {row["count"]:,}')

    return pairs_df

# -----------------------------------------------------------------------------
# STAGE 6 — SPLIT & SAVE
# Product-level split: same product never appears in both train and test
# -----------------------------------------------------------------------------
def split_and_save(spark, pairs_df):
    log.info('=' * 60)
    log.info('STAGE 6 — Product-level split and save')
    log.info('=' * 60)

    product_ids    = pairs_df.select('product_id').distinct()
    total_products = product_ids.count()
    log.info(f'Unique products: {total_products:,}')

    p_train, p_val, p_test = product_ids.randomSplit([0.8, 0.1, 0.1], seed=RANDOM_SEED)

    train_df = pairs_df.join(
        p_train.withColumnRenamed('product_id', '_pid'),
        pairs_df.product_id == F.col('_pid'), 'inner'
    ).drop('_pid')
    val_df = pairs_df.join(
        p_val.withColumnRenamed('product_id', '_pid'),
        pairs_df.product_id == F.col('_pid'), 'inner'
    ).drop('_pid')
    test_df = pairs_df.join(
        p_test.withColumnRenamed('product_id', '_pid'),
        pairs_df.product_id == F.col('_pid'), 'inner'
    ).drop('_pid')

    train_count = train_df.count()
    val_count   = val_df.count()
    test_count  = test_df.count()
    total       = train_count + val_count + test_count

    log.info(f'Train: {train_count:,} ({train_count / total * 100:.1f}%)')
    log.info(f'Val:   {val_count:,} ({val_count / total * 100:.1f}%)')
    log.info(f'Test:  {test_count:,} ({test_count / total * 100:.1f}%)')

    cols = ['instruction', 'input', 'output', 'type',
            'skin_type', 'problem', 'budget', 'product_id', 'text']

    train_df.select(cols).write.mode('overwrite').parquet(f'{OUT_BASE}/train/')
    val_df.select(cols).write.mode('overwrite').parquet(f'{OUT_BASE}/val/')
    test_df.select(cols).write.mode('overwrite').parquet(f'{OUT_BASE}/test/')

    log.info(f'Train saved to {OUT_BASE}/train/')
    log.info(f'Val   saved to {OUT_BASE}/val/')
    log.info(f'Test  saved to {OUT_BASE}/test/')

    sample = train_df.select(cols).limit(SAMPLE_PREVIEW_N).toPandas()
    sample_path = '/tmp/sample_preview.jsonl'
    with open(sample_path, 'w', encoding='utf-8') as f:
        for _, row in sample.iterrows():
            f.write(json.dumps(row.to_dict(), ensure_ascii=False) + '\n')
    boto3.client('s3').upload_file(
        sample_path, BUCKET_NAME, 'processed/sample_preview.jsonl'
    )
    log.info(f'Preview saved to s3://{BUCKET_NAME}/processed/sample_preview.jsonl')

    return train_df, val_df, test_df, train_count, val_count, test_count

# -----------------------------------------------------------------------------
# STAGE 7 — EDA FIGURES
# Requires matplotlib installed via bootstrap.sh
# -----------------------------------------------------------------------------
def generate_eda(spark, pairs_df, product_agg, train_count, val_count, test_count):
    log.info('=' * 60)
    log.info('STAGE 7 — Generating EDA figures')
    log.info('=' * 60)

    s3 = boto3.client('s3')

    def upload_fig(local_path, s3_key):
        s3.upload_file(local_path, BUCKET_NAME, s3_key)
        log.info(f'Uploaded to s3://{BUCKET_NAME}/{s3_key}')

    # Figure 1: Q&A type + skin type + budget distribution
    type_counts   = pairs_df.groupBy('type').count().toPandas().set_index('type')['count'].to_dict()
    skin_counts   = (
        pairs_df.filter(F.col('skin_type').isNotNull())
        .groupBy('skin_type').count().orderBy('count', ascending=False).limit(6).toPandas()
    )
    budget_counts = pairs_df.groupBy('budget').count().toPandas()

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle(
        f'EDA Figure 1 — {NETID} Beauty Dataset\nTotal Q&A Pairs: {pairs_df.count():,}',
        fontsize=13, fontweight='bold'
    )

    type_order  = ['user_aware_recommendation', 'reasoned_sentiment_summary', 'specific_reasoned_qa']
    type_labels = ['User-Aware\nRecommendation', 'Reasoned\nSummary', 'Specific\nQ&A']
    type_vals   = [type_counts.get(t, 0) for t in type_order]
    bars = axes[0].bar(type_labels, type_vals,
                       color=['#3498db', '#e67e22', '#2ecc71'], edgecolor='white')
    axes[0].set_title('Q&A Pairs by Type')
    axes[0].set_ylabel('Count')
    for bar, val in zip(bars, type_vals):
        axes[0].text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(type_vals) * 0.01,
            f'{val:,}', ha='center', fontsize=9, fontweight='bold'
        )

    if not skin_counts.empty:
        axes[1].barh(
            skin_counts['skin_type'].str.replace('_', ' ').str.title(),
            skin_counts['count'], color='#e74c3c'
        )
        axes[1].set_title('Pairs by Skin Type')

    if not budget_counts.empty:
        axes[2].bar(budget_counts['budget'], budget_counts['count'],
                    color='#9b59b6', edgecolor='white')
        axes[2].set_title('Pairs by Budget Tier')
        plt.setp(axes[2].xaxis.get_majorticklabels(), rotation=15, ha='right')

    plt.tight_layout()
    fig.savefig('/tmp/eda_figure_1.png', dpi=150, bbox_inches='tight')
    plt.close(fig)
    upload_fig('/tmp/eda_figure_1.png', 'eda/eda_figure_1.png')

    # Figure 2: Split counts + product rating + reviews per product
    avg_ratings      = product_agg.select('avg_rating').toPandas()['avg_rating'].dropna()
    review_counts_pd = product_agg.select('review_count').toPandas()['review_count'].dropna()

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle('EDA Figure 2 — Dataset Split and Product Statistics',
                 fontsize=13, fontweight='bold')

    split_vals = [train_count, val_count, test_count]
    bars = axes[0].bar(
        ['Train (80%)', 'Val (10%)', 'Test (10%)'], split_vals,
        color=['#3498db', '#e67e22', '#2ecc71'], edgecolor='white'
    )
    axes[0].set_title('Train / Val / Test Split (Product-Level)')
    axes[0].set_ylabel('Number of Q&A Pairs')
    for bar, val in zip(bars, split_vals):
        axes[0].text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(split_vals) * 0.01,
            f'{val:,}', ha='center', fontsize=9, fontweight='bold'
        )

    axes[1].hist(avg_ratings, bins=40, color='#f39c12', edgecolor='white')
    axes[1].set_title('Product Average Rating Distribution')
    axes[1].set_xlabel('Average Rating')
    axes[1].axvline(avg_ratings.mean(), color='red', linestyle='--',
                    label=f'Mean: {avg_ratings.mean():.2f}')
    axes[1].legend()

    axes[2].hist(review_counts_pd.clip(upper=100), bins=40, color='#1abc9c', edgecolor='white')
    axes[2].set_title('Reviews per Product (capped at 100)')
    axes[2].set_xlabel('Review Count')

    plt.tight_layout()
    fig.savefig('/tmp/eda_figure_2.png', dpi=150, bbox_inches='tight')
    plt.close(fig)
    upload_fig('/tmp/eda_figure_2.png', 'eda/eda_figure_2.png')

    # Figure 3: Output length + verified purchase % + Q&A type pie
    output_lengths = (
        pairs_df
        .withColumn('out_words', F.size(F.split(F.col('output'), ' ')))
        .select('out_words').toPandas()['out_words'].dropna()
    )
    verified_pcts = product_agg.select('verified_pct').toPandas()['verified_pct'].dropna()
    type_pie      = pairs_df.groupBy('type').count().toPandas()

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle('EDA Figure 3 — Output Quality and Purchase Verification',
                 fontsize=13, fontweight='bold')

    axes[0].hist(output_lengths.clip(upper=300), bins=40, color='#8e44ad', edgecolor='white')
    axes[0].set_title('Output Length Distribution (words)')
    axes[0].set_xlabel('Word Count')
    axes[0].axvline(output_lengths.median(), color='red', linestyle='--',
                    label=f'Median: {output_lengths.median():.0f} words')
    axes[0].legend()

    axes[1].hist(verified_pcts, bins=30, color='#e74c3c', edgecolor='white')
    axes[1].set_title('Verified Purchase % per Product')
    axes[1].set_xlabel('Verified Purchase %')

    if not type_pie.empty:
        short_labels = {
            'user_aware_recommendation' : 'Recommendation',
            'reasoned_sentiment_summary': 'Sentiment Summary',
            'specific_reasoned_qa'      : 'Specific Q&A',
        }
        type_pie['label'] = type_pie['type'].map(lambda x: short_labels.get(x, x))
        axes[2].pie(
            type_pie['count'], labels=type_pie['label'],
            autopct='%1.1f%%', colors=['#3498db', '#e67e22', '#2ecc71'], startangle=90
        )
        axes[2].set_title('Q&A Type Distribution')

    plt.tight_layout()
    fig.savefig('/tmp/eda_figure_3.png', dpi=150, bbox_inches='tight')
    plt.close(fig)
    upload_fig('/tmp/eda_figure_3.png', 'eda/eda_figure_3.png')

    log.info('All 3 EDA figures uploaded to S3.')

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main():
    start_time = datetime.now()
    log.info('=' * 60)
    log.info('25htc5 Beauty Assistant — PySpark Preprocessing Pipeline')
    log.info(f'Started    : {start_time.strftime("%Y-%m-%d %H:%M:%S")}')
    log.info(f'Bucket     : {BUCKET}')
    log.info(f'Max reviews: {MAX_REVIEWS:,}' if MAX_REVIEWS else 'Max reviews: None (full dataset)')
    log.info('=' * 60)

    spark = create_spark_session()

    reviews_raw, meta_raw = load_data(spark)
    reviews_clean         = clean_reviews(reviews_raw)
    joined                = clean_and_join(spark, reviews_clean, meta_raw)
    product_agg           = aggregate_products(spark, joined)
    pairs_df              = generate_qa_pairs(spark, product_agg)

    train_df, val_df, test_df, train_count, val_count, test_count = \
        split_and_save(spark, pairs_df)

    generate_eda(spark, pairs_df, product_agg, train_count, val_count, test_count)

    elapsed = datetime.now() - start_time
    log.info('=' * 60)
    log.info('PIPELINE COMPLETE')
    log.info(f'Train: {train_count:,} | Val: {val_count:,} | Test: {test_count:,}')
    log.info(f'Total time : {elapsed}')
    log.info(f'Output     : {OUT_BASE}/')
    log.info(f'EDA        : {EDA_S3}/')
    log.info('=' * 60)

    spark.stop()
    log.info('Spark session stopped. Pipeline finished successfully.')


if __name__ == '__main__':
    main()
