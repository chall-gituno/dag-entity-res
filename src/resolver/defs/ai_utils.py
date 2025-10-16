import os
from openai import OpenAI
from openai import OpenAIError
from pathlib import Path
from jinja2 import Environment, PackageLoader, select_autoescape

# Load templates from resolver.defs.prompts
ai_env = Environment(loader=PackageLoader("resolver.defs", "prompts"),
                     autoescape=select_autoescape([]))


def render_prompt(template_name: str, params: dict = {}) -> str:
  return ai_env.get_template(template_name).render(params)


def call_ai(prompt: str, model: str = "gpt-3.5-turbo") -> str:
  """
    Minimal wrapper that sends prompt to OpenAI and returns the assistant text.
    Returns an error message if something goes wrong.  
    """
  api_key = os.environ.get("OPENAI_API_KEY")
  if not api_key:
    return "ERROR: OPENAI_API_KEY not set"

  client = OpenAI(api_key=api_key)
  try:
    resp = client.chat.completions.create(
      model=model,
      messages=[{
        "role": "user",
        "content": prompt
      }],
      temperature=0.1,
    )
    return resp.choices[0].message.content
  except OpenAIError as e:
    return f"ERROR: OpenAI call failed: {e}"
