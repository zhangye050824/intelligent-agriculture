# -*- coding: utf-8 -*-
path = r'e:\大四实训\最终项目\project\templates\index.html'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# 找到 "// Tab 5: 偏差" 后面紧跟着 "const biasChart = echarts" 或 "let biasChart"
OLD_MARK = '    // Tab 5: 偏差\n    const biasChart = echarts.init(document.getElementById(\'biasChart\'));'
NEW_MARK = '    // Tab 5: 偏差（懒初始化）\n    let biasChart = null;\n    function ensureBiaschart() {\n        if (!biasChart) {\n            biasChart = echarts.init(document.getElementById(\'biasChart\'));'

if OLD_MARK in content:
    start = content.find(OLD_MARK)
    # 找到从 setOption({ 到 }); 的完整块
    # 在 start 之后找 setOption({
    block_start = content.find('biasChart.setOption({', start)
    if block_start == -1:
        print("ERROR: can't find setOption block")
    else:
        # 找对应的 });
        depth = 0
        i = block_start + len('biasChart.setOption({') - 1  # 从 { 开始
        while i < len(content):
            c = content[i]
            if c == '{': depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    # 找后面的 );
                    j = content.find(';', i)
                    # 找这之后的空行 + // renderEngine
                    end_marker = '\n\n    // renderEngine'
                    end_pos = content.find(end_marker, j)
                    old_full = content[start:end_pos]
                    # 替换
                    # 先把 OLD_MARK 替换成 NEW_MARK
                    new_full = old_full.replace(OLD_MARK, NEW_MARK)
                    # 然后在最后的 }); 后面补 }}\n
                    closing_idx = new_full.rfind('});')
                    if closing_idx != -1:
                        new_full = new_full[:closing_idx+3] + '\n        }\n    }' + new_full[closing_idx+3:]
                    content = content[:start] + new_full + content[end_pos:]
                    with open(path, 'w', encoding='utf-8') as f:
                        f.write(content)
                    print("SUCCESS! Replaced biasChart block.")
                    print("New block preview:")
                    print(new_full[:200])
                    print("...")
        i += 1
else:
    print("OLD_MARK not found - already replaced?")
    # 检查是否已经是 let
    if 'let biasChart = null' in content and 'function ensureBiaschart()' in content:
        print("Good - already lazy initialized!")
    else:
        print("Unknown state, need manual fix")
