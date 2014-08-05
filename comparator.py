from sys import argv
import tables


class Comparator(object):
    ROOT_PATH = "/"
    SIGNIFICANT_DS = "books"

    # ignoring timestamp for now
    SIGNIFICANT_PROPERTIES = ["ask", "bid", "seqnum"]

    def __init__(self, left_file, right_file):
        self.left = left_file
        self.right = right_file

        self.wrong_count = 0

    # we'll take the group list from the lhs
    def compare_files():
        for group in left.walk_groups(ROOT_PATH):
            lhs_ds = set_ds(self.left, group)
            rhs_ds = set_ds(self.right, group)

            if not compare_ds(lhs_ds, rhs_ds):
                self.wrong_count += 1

    def set_ds(h5file, group):
        try:
            return h5file.get_node(group._v_pathname, SIGNIFICANT_DS)
        except NoSuchNodeError:
            return None

    def compare_row(left_row, right_row):
        for prop in self.SIGNIFICANT_PROPERTIES:
            try:
                ans = (left_row[prop] == right_row[prop]).all()
            except AttributeError:
                ans = left_row[prop] == right_row[prop]

            if not ans:
                print("Mismatch. Left: {}, right: {}".format(left_row[prop], right_row[prop]))

            return ans



    def compare_ds(lhs_ds, rhs_ds):
        if(rhs_ds == None):
            print("RHS dataset doesn't exist: {}".format(lhs_ds))
            return False
        else:
            for left_row, right_row in zip(lhs_ds.iterrows(), rhs_ds.iterrows()):
                if not compare_row(left_row, right_row):
                    return False
            return True

def main(left_path, right_path):
    comp = Comparator(tables.open(left_path, "r"), tables.open(right_path, "r"))


if __name__ == '__main__':
