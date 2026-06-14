import csv
import io
from mrjob.job import MRJob
from mrjob.step import MRStep

IDX_UF        = 2
IDX_MUNICIPIO = 4
IDX_VALOR     = 8

ESTADOS_Q4 = {"SP", "BA"}
ESTADOS_Q5 = {"SP", "MG", "RJ"}

DELIMITER = ";"

def parse_line(raw_line):
    if isinstance(raw_line, bytes):
        line = raw_line.decode("latin-1", errors="replace")
    else:
        line = raw_line

    line = line.strip()
    if not line:
        return None

    reader = csv.reader(io.StringIO(line), delimiter=DELIMITER, quotechar='"')
    fields = next(reader, None)

    if fields is None or len(fields) <= IDX_VALOR:
        return None

    primeiro = fields[0].strip().strip('"')
    if not primeiro.isdigit():
        return None

    return fields

def parse_valor(raw):
    v = raw.strip().strip('"').replace(".", "").replace(",", ".")
    try:
        return float(v)
    except ValueError:
        return 0.0

class BolsaFamiliaAnalytics(MRJob):

    def mapper_step1(self, _, line):
        fields = parse_line(line)
        if fields is None:
            return

        uf        = fields[IDX_UF].strip().strip('"').upper()
        municipio = fields[IDX_MUNICIPIO].strip().strip('"').upper()
        valor     = parse_valor(fields[IDX_VALOR])

        if not uf or not municipio:
            return

        yield ("Q1", uf), 1
        yield ("Q2", f"{uf}|{municipio}"), 1
        yield ("Q3", uf), valor

        if uf in ESTADOS_Q4:
            yield ("Q4", f"{uf}|{municipio}"), valor

        if uf in ESTADOS_Q5:
            yield ("Q5", f"{uf}|{municipio}"), 1

    def reducer_step1(self, key, values):
        total = sum(values)
        yield key[0], (total, key[1])

    def mapper_step2(self, query, value_key):
        total, chave = value_key
        yield query, (-total, chave)

    def reducer_step2(self, query, pairs):
        LIMITS = {"Q1": None, "Q2": 50, "Q3": None, "Q4": 10, "Q5": 100}
        limit  = LIMITS.get(query)

        sorted_pairs = sorted(pairs, key=lambda x: x[0])

        for rank, (neg_total, chave) in enumerate(sorted_pairs, start=1):
            if limit and rank > limit:
                break
            yield query, {"rank": rank, "chave": chave, "valor": -neg_total}

    def steps(self):
        return [
            MRStep(mapper=self.mapper_step1,  reducer=self.reducer_step1),
            MRStep(mapper=self.mapper_step2,  reducer=self.reducer_step2),
        ]

if __name__ == "__main__":
    BolsaFamiliaAnalytics.run()
