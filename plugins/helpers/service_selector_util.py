class ServiceSelector:
    keywords = ["WAP","wireless access point","wireless points","access point"]

    def is_applicable_request(description):
        deliberation = False

        if description is not None:
            idx = 0
            end = len(ServiceSelector.keywords)

            while not deliberation and idx < end:
                if description.lower().find(ServiceSelector.keywords[idx].lower()) > -1:
                    deliberation = True
                else:
                    idx += 1

        return deliberation

    def replace_with_other_value(pair):
        pass_back = pair[0]

        if ((pair[1] is not None) and (len(pair[1]) > 0)):
            pass_back = f"{pair[0]}: {pair[1]}"

        return pass_back