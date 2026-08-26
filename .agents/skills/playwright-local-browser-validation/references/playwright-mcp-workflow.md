# Playwright MCP workflow reference

## Navegação
- browser_navigate
- browser_navigate_back
- browser_tabs
- browser_close

## Inspeção
- browser_snapshot
- browser_console_messages
- browser_network_requests
- browser_network_request
- browser_evaluate

## Interação
- browser_click
- browser_type
- browser_fill_form
- browser_press_key
- browser_select_option
- browser_hover
- browser_drag
- browser_drop
- browser_file_upload
- browser_handle_dialog

## Visual
- browser_resize
- browser_take_screenshot
- browser_wait_for

## Perigosa / excepcional
- browser_run_code_unsafe

Use snapshot para interação normal.
Use screenshot para validar visual.
Use console para erros JS.
Use network para API/assets.
Use resize para desktop/mobile.
Use view_image em screenshots locais quando a validação depender do que aparece visualmente.
