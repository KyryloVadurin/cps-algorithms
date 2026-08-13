import re

def on_page_markdown(markdown, page, config, files):
    # 1. Автоматично перетворюємо блоки ```math ... ``` на $$ ... $$
    markdown = re.sub(r'```math\s*\n([\s\S]*?)\n```', r'\n$$\n\1\n$$\n', markdown)

    # 2. Автоматично додаємо порожній рядок перед списками (- або * або 1.), якщо його немає
    markdown = re.sub(r'([^\n])\n([ \t]*[-*+]|\d+\.)\s+', r'\1\n\n\2 ', markdown)

    # 3. Замінюємо знак '<' у математичних блоках $$ на '\lt', щоб не ламався HTML
    def fix_math_tags(match):
        math_content = match.group(0)
        return re.sub(r'<(\s*[0-9a-zA-Z_])', r'\\lt \1', math_content)

    markdown = re.sub(r'\$\$[\s\S]*?\$\$', fix_math_tags, markdown)

    return markdown
