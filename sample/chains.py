from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import PromptTemplate
from dotenv import load_dotenv

load_dotenv()

chain_genai = ChatGoogleGenerativeAI(model="gemini-1.5-flash")
chain_with_prompt = (
    PromptTemplate(
        template="Translate the following into {language}", input_variables=["language"]
    )
    | chain_genai
)
