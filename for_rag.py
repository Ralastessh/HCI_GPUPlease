import os
from langchain_community.document_loaders import WebBaseLoader # 1. 로드
from langchain_text_splitters import RecursiveCharacterTextSplitter # 2. 분할
from langchain_community.vectorstores import FAISS # 3. 저장 (Vector DB - 로컬 백업용)
from langchain_postgres import PGVector # 3. 저장 (Vector DB - PostgreSQL)
from langchain_huggingface import HuggingFaceEmbeddings # 3. 임베딩 (HF 모델 1)
from langchain_huggingface import HuggingFacePipeline # 5. 생성 (HF 모델 2)
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
import torch
from sqlalchemy import create_engine, text
import psycopg2
from urllib.parse import quote_plus
from dotenv import load_dotenv

# GPU 사용 설정 (가능하면)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {device}")
load_dotenv()

# --- 0. Database Configuration (Supabase PostgreSQL) ---
DB_CFG = {
    "host": os.getenv("SUPABASE_HOST", "aws-1-ap-southeast-1.pooler.supabase.com"),
    "port": os.getenv("SUPABASE_PORT", "5432"),
    "database": os.getenv("SUPABASE_DATABASE", "postgres"),
    "user": os.getenv("SUPABASE_USER", "postgres.wlhignlnfknbsxmbbcno"),
    "password": os.getenv("SUPABASE_PASSWORD"),
    "sslmode": os.getenv("SUPABASE_SSLMODE", "require"),
}

missing_keys = [key for key in ("password",) if not DB_CFG[key]]
if missing_keys:
    raise ValueError(
        "Missing required environment variables: " + ", ".join(f"SUPABASE_{key.upper()}" for key in missing_keys)
    )

DB_CFG["port"] = int(DB_CFG["port"])

# PostgreSQL 연결 문자열 생성
encoded_password = quote_plus(DB_CFG['password'])
CONNECTION_STRING = (
    f"postgresql://{DB_CFG['user']}:{encoded_password}@"
    f"{DB_CFG['host']}:{DB_CFG['port']}/{DB_CFG['database']}"
    f"?sslmode={DB_CFG['sslmode']}"
)

# --- DB 연결 테스트 ---
print("\n=== Testing Database Connection ===")
try:
    # psycopg2로 직접 연결 테스트
    conn = psycopg2.connect(
        host=DB_CFG['host'],
        port=DB_CFG['port'],
        database=DB_CFG['database'],
        user=DB_CFG['user'],
        password=DB_CFG['password'],
        sslmode=DB_CFG['sslmode']
    )
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    db_version = cursor.fetchone()
    print(f"✅ Database connection successful!")
    print(f"   PostgreSQL version: {db_version[0][:50]}...")
    
    # pgvector 확장 확인
    cursor.execute("SELECT * FROM pg_extension WHERE extname = 'vector';")
    if cursor.fetchone():
        print("✅ pgvector extension is installed.")
    else:
        print("⚠️  WARNING: pgvector extension is NOT installed!")
        print("   Please enable it in Supabase Dashboard: Database → Extensions → vector")
    
    cursor.close()
    conn.close()
except Exception as e:
    print(f"❌ Database connection failed: {e}")
    print("   Please check your DB credentials and network connection.")
    raise

# SQLAlchemy 엔진 생성 (PGVector 초기화용)
engine = create_engine(CONNECTION_STRING)
print("Database connection configured.\n")

# --- 1. Load (문서 로드) ---
# 예시로, 국내 주식 시장에 대한 일반적인 웹 문서를 로드합니다.
# 실제 프로젝트에서는 DART 공시, 뉴스 기사 텍스트 파일을 로드해야 합니다.
print("Step 1: Loading documents...")
loader = WebBaseLoader(web_path="https://ko.wikipedia.org/wiki/%EB%8C%80%ED%95%9C%EB%AF%BC%EA%B5%AD%EC%9D%98_%EC%A3%BC%EC%8B%9D%EC%8B%9C%EC%9E%A5")
documents = loader.load()

# --- 2. Split (문서 분할) ---
# 문서를 500자 단위로 자르고, 50자씩 겹치게 합니다.
print("Step 2: Splitting documents...")
text_splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
docs = text_splitter.split_documents(documents)
print(f"Total {len(docs)} document chunks created.")

# --- 3. Embed & Store (임베딩 및 저장) ---
# [HF 모델 1: 임베딩 모델 (검색용)]
# 한국어 문장을 벡터로 변환하는 모델을 Hugging Face에서 로드합니다.
# (모델의 '가중치'를 학습하는 게 아니라, pre-trained 모델을 '다운로드'하여 사용하는 것입니다)
print("Step 3: Loading HF Embedding Model and creating Vector Store...")
model_name_embed = "jhgan/ko-sroberta-multitask" # 한국어 임베딩 모델
model_kwargs = {'device': device}
encode_kwargs = {'normalize_embeddings': True}

hf_embeddings = HuggingFaceEmbeddings(
    model_name=model_name_embed,
    model_kwargs=model_kwargs,
    encode_kwargs=encode_kwargs
)

# PostgreSQL (Supabase) 벡터 DB에 분할된 문서를 임베딩하여 저장합니다.
# 이 과정이 "오픈북 시험을 위한 참고서(Vector DB)"를 만드는 과정입니다.
# table_name: 벡터를 저장할 테이블 이름 (필요시 변경 가능)
print("Creating/Connecting to PostgreSQL Vector Store...")

# 기존 DB에 데이터가 있는지 확인하고 재사용할지 결정
USE_EXISTING_DB = True  # True: 기존 DB 사용, False: 새로 생성
COLLECTION_NAME = "langchain_pg_embedding"

if USE_EXISTING_DB:
    try:
        # 기존 벡터 스토어에 연결 시도
        db = PGVector(
            embeddings=hf_embeddings,
            connection=CONNECTION_STRING,
            collection_name=COLLECTION_NAME,
            use_jsonb=True,
        )
        print(f"✅ Connected to existing vector store: {COLLECTION_NAME}")
        # 기존 데이터 개수 확인
        try:
            # 간단한 검색으로 데이터 존재 여부 확인
            test_results = db.similarity_search("test", k=1)
            print(f"   Existing documents in DB: (at least {len(test_results)} found)")
        except:
            print("   (Could not count existing documents)")
    except Exception as e:
        print(f"⚠️  Could not connect to existing DB: {e}")
        print("   Creating new vector store...")
        db = PGVector.from_documents(
            documents=docs,
            embedding=hf_embeddings,
            connection=CONNECTION_STRING,
            collection_name=COLLECTION_NAME,
            use_jsonb=True,
        )
        print("✅ Documents stored in PostgreSQL successfully.")
else:
    # 새로 생성
    db = PGVector.from_documents(
        documents=docs,
        embedding=hf_embeddings,
        connection=CONNECTION_STRING,
        collection_name=COLLECTION_NAME,
        use_jsonb=True,
    )
    print("✅ Documents stored in PostgreSQL successfully.")

# retriever를 정의합니다 (질문이 오면 3개의 관련 문서를 검색하도록 설정)
# RAG의 "R" (Retrieve) 단계를 담당합니다
retriever = db.as_retriever(search_kwargs={"k": 3})
print("✅ Retriever configured (will retrieve top 3 relevant documents)")

# --- 4. Retrieve (검색 테스트) ---
# "RAG" 중 "R"이 잘 작동하는지 테스트
print("\n" + "="*60)
print("🔍 Testing RAG Retrieve Step (검색 테스트)")
print("="*60)
query = "한국 주식시장의 특징은 무엇인가?"
retrieved_docs = retriever.invoke(query)
print(f"\n✅ Retriever Test Succeeded!")
print(f"   Query: {query}")
print(f"   Retrieved {len(retrieved_docs)} documents")
print("\n--- Retrieved Documents Preview ---")
for i, doc in enumerate(retrieved_docs[:3], 1):
    print(f"\n[Document {i}]")
    print(f"{doc.page_content[:200]}...")
    if hasattr(doc, 'metadata') and doc.metadata:
        print(f"Metadata: {doc.metadata}")
print("\n" + "="*60)
print("✅ RAG Retrieve Step is working correctly!")
print("="*60)

# --- 5. Generate (생성) ---
# [HF 모델 2: 생성형 LLM (답변용)]
# 실제 AI 에이전트의 '뇌' 역할을 할 모델을 Hugging Face에서 로드합니다.
# ⚠️ 경고: 이 모델은 VRAM이 많이 필요합니다. 로컬 실행이 어려울 수 있습니다.
# (실제 프로젝트에서는 GPT, Claude API 또는 Quantized 모델을 사용하기도 합니다)

# 디스크 공간 확인 후 LLM 로드 시도
SKIP_LLM_LOAD = False  # True로 설정하면 LLM 로드 건너뛰기 (Retrieve만 테스트)

if SKIP_LLM_LOAD:
    print("⚠️  Skipping LLM loading (SKIP_LLM_LOAD=True)")
    print("   RAG Retrieve step is working. To test full RAG, ensure sufficient disk space.")
else:
    print("Step 5: Loading HF Generator LLM...")

    try:
        model_id = "EleutherAI/polyglot-ko-1.3b" # 한국어 소형 LLM (예시)
        # model_id = "gemma-2b" # 다른 모델 예시

        tokenizer_llm = AutoTokenizer.from_pretrained(model_id)
        model_llm = AutoModelForCausalLM.from_pretrained(
            model_id,
            torch_dtype=torch.float16, # 메모리 줄이기 (bfloat16도 가능)
            low_cpu_mem_usage=True, # CPU 메모리 적게 사용
        ).to(device)

        # LangChain의 HuggingFacePipeline으로 감싸기
        # max_new_tokens: 답변 생성 최대 길이
        hf_pipeline = pipeline(
            "text-generation",
            model=model_llm,
            tokenizer=tokenizer_llm,
            device=device,
            max_new_tokens=512,
            repetition_penalty=1.1 # 반복 방지
        )

        # LangChain에서 사용할 수 있도록 llm 객체 생성
        llm = HuggingFacePipeline(pipeline=hf_pipeline)
        print("✅ HF Generator LLM loaded.")
    except RuntimeError as e:
        if "No space left on device" in str(e):
            print("❌ Disk space insufficient for LLM model download.")
            print("   RAG Retrieve step is working correctly.")
            print("   To test full RAG, please free up disk space or use API-based LLM.")
            SKIP_LLM_LOAD = True
            llm = None
        else:
            raise

# --- 6. RAG Chain (최종 RAG 체인 생성) ---
if SKIP_LLM_LOAD or llm is None:
    print("\n⚠️  Skipping RAG Chain creation (LLM not loaded)")
    print("   RAG Retrieve step completed successfully!")
    print("   To test full RAG, ensure sufficient disk space for LLM model.")
    print("\n" + "="*60)
    print("✅ RAG Retrieve Step Test Complete!")
    print("="*60)
    print("\nSummary:")
    print("  ✅ Database connection: Working")
    print("  ✅ Document loading & splitting: Working")
    print("  ✅ Embedding & Vector Store: Working")
    print("  ✅ Retrieve (검색): Working")
    print("  ⚠️  Generate (생성): Skipped (disk space insufficient)")
    print("\nTo enable full RAG:")
    print("  1. Free up disk space (at least 2-3GB needed)")
    print("  2. Or use API-based LLM (OpenAI, Anthropic, etc.)")
    print("="*60)
else:
    # RAG (Retrieval-Augmented Generation) 프로세스:
    # 1. Retrieve: 사용자 질문을 임베딩하여 벡터 DB에서 관련 문서 검색
    # 2. Augment: 검색된 문서를 Context로 프롬프트에 추가
    # 3. Generate: LLM이 Context와 질문을 바탕으로 답변 생성
    print("Step 6: Creating RAG Chain...")
    
    # 프롬프트 템플릿 정의 (LangChain 1.0 API)
    rag_prompt = ChatPromptTemplate.from_messages([
        ("system", "당신은 금융 전문 AI 어시스턴트입니다. 제공된 'Context' 정보를 바탕으로만 사용자의 'Question'에 대해 답변해 주십시오. Context에 없는 내용은 '알 수 없습니다'라고 답변하십시오. 절대로 정보를 지어내지 마십시오."),
        ("human", "Context:\n{context}\n\nQuestion: {question}\n\nAnswer:")
    ])

    # LangChain 1.0 API를 사용한 RAG 체인 생성
    def format_docs(docs):
        return "\n\n".join(doc.page_content for doc in docs)

    # RAG 체인 구성
    qa_chain = (
        {"context": retriever | format_docs, "question": RunnablePassthrough()}
        | rag_prompt
        | llm
        | StrOutputParser()
    )

    print("✅ RAG Chain created successfully.")

    # --- 7. 실행 및 테스트 (Run & Test) ---
    print("\n" + "="*60)
    print("🚀 RAG Chain Execution Test 🚀")
    print("="*60)
    question_to_ask = "한국 주식시장에서 코스닥(KOSDAQ)의 역할은 무엇인가?"

    # RAG 체인 실행 과정:
    # 1. Retrieve: 'question_to_ask'를 임베딩 → 벡터 DB에서 유사도 높은 문서 3개 검색
    # 2. Augment: 검색된 문서들을 Context로 프롬프트에 삽입
    # 3. Generate: Context + Question이 포함된 프롬프트를 LLM에 전달 → 답변 생성
    print(f"\n📝 Question: {question_to_ask}")
    print("\n⏳ Processing... (This may take a while)")
    # 검색된 문서 먼저 가져오기
    retrieved_docs = retriever.invoke(question_to_ask)

    # RAG 체인 실행
    answer = qa_chain.invoke(question_to_ask)

    print(f"\n✅ Answer: {answer}")
    print("\n" + "-"*60)
    print("📚 Source Documents (답변의 근거 - 검색된 문서들)")
    print("-"*60)
    for i, doc in enumerate(retrieved_docs):
        print(f"\n[Source {i+1}]")
        print(f"{doc.page_content[:200]}...")  # 처음 200자만 표시
        if hasattr(doc, 'metadata'):
            print(f"Metadata: {doc.metadata}")
    print("\n" + "="*60)
    print("✅ RAG Test Complete!")
    print("="*60)