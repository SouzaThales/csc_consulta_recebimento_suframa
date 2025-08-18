import functools

def log_exceptions(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            tb = e.__traceback__

            frames = []
            while tb:
                frame = tb.tb_frame
                lineno = tb.tb_lineno
                func_name = frame.f_code.co_name
                frames.append((func_name, lineno))
                tb = tb.tb_next

            # Remove o wrapper
            frames = [(f, l) for f, l in frames if f != 'wrapper']

            original_msg = str(e)
            if len(original_msg) > 200:
                original_msg = original_msg[:200] + "..."

            # Construir mensagem do traceback concatenada corretamente
            trace_msg = " -> ".join(
                [f"Erro {func_name} ({lineno}) " for func_name, lineno in frames]
            )
            raise Exception(f"{trace_msg}: {original_msg} ") from None
    return wrapper