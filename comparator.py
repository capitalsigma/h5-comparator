import tables
import itertools

from sys import argv


class Comparator(object):
    ROOT_PATH = "/"
    SIGNIFICANT_DS = "books"

    # ignoring timestamp for now
    SIGNIFICANT_PROPERTIES = ["ask", "bid", "seqnum"]

    def __init__(self, left_file, right_file, symbols):
        self.left = left_file
        self.right = right_file
        self.symbols = symbols

        self.wrong_count = 0

    # we'll take the group list from the lhs
    def compare_files(self):
        for ticker in self.symbols:
            try:
                group = self.left.get_node("/", ticker)
            except tables.NoSuchNodeError:
                continue

            lhs_ds = self.get_ds(self.left, group)
            rhs_ds = self.get_ds(self.right, group)

            if not self.compare_ds(lhs_ds, rhs_ds):
                self.wrong_count += 1

        return self.wrong_count

    def get_ds(self, h5file, group):
        print("got group: {}".format(group))
        try:
            return h5file.get_node(group._v_pathname, self.SIGNIFICANT_DS)
        except tables.NoSuchNodeError:
            return None

    def compare_row(self, left_row, right_row):
        # print("left row: {}, right row: {}".format(left_row, right_row))
        for prop in self.SIGNIFICANT_PROPERTIES:
            try:
                ans = (left_row[prop] == right_row[prop]).all()
            except AttributeError:
                ans = left_row[prop] == right_row[prop]
            except TypeError:
                ans = left_row == right_row

            if not ans:
                try:
                    lhs_el = left_row[prop]
                    rhs_el = right_row[prop]
                except TypeError:
                    lhs_el = left_row
                    rhs_el = right_row

                print("Mismatch. Left row: {}, right row: {}".format(
                    left_row, right_row))
                print("Values for prop {}: \nleft={}, \nright={}".format(
                    prop, lhs_el, rhs_el))

                try:
                    print("Error is at seqnum {}".format(left_row['seqnum']))
                except AttributeError:
                    pass

                return False
        return True




    def compare_ds(self, lhs_ds, rhs_ds):
        if rhs_ds == None:
            print("RHS dataset doesn't exist: {}".format(lhs_ds))
            return False
        else:
            for left_row, right_row in itertools.izip_longest(lhs_ds, rhs_ds):
                if not self.compare_row(left_row, right_row):
                    return False
            return True

def main(left_path, right_path):
    symbols = [
        # Index ETFs
        'SPY', 'DIA', #'QQQ',
        # Sectors
        'XLK', 'XLF', 'XLP', 'XLE', 'XLY', 'XLV', 'XLB',
        # Vanguard
        'VCR', 'VDC', 'VHT', 'VIS', 'VAW', 'VNQ', 'VGT', 'VOX', 'VPU',
        # Energy
        'XOM', 'RDS', 'BP',
        # Home Improvement
        'HD', 'LOW', 'XHB',
        # Banks
        'MS', 'GS', 'BAC', 'JPM', 'C',
        # Exchanges
        'CME', 'NYX',
        # Big Techs
        'AAPL', 'MSFT', 'GOOG', 'CSCO',
        'GE', 'CVX', 'JNJ', 'IBM', 'PG', 'PFE',
    ]

    comp = Comparator(tables.open_file(left_path, "r"),
                      tables.open_file(right_path, "r"),
                      symbols)

    count = comp.compare_files()
    print("Wrong count: {}".format(count))




if __name__ == '__main__':
    main(argv[1], argv[2])
