FROM rocker/tidyverse@sha256:6a7c913590e758b5fe2ad9921ccc5df7c7160e5de1db5f353630fe8e0ee2f876

RUN mkdir -p /input_data
RUN mkdir -p /results
RUN mkdir -p /diagnostics
VOLUME /results
VOLUME /input_data
VOLUME /diagnostics
COPY dummy_step.R .

ENTRYPOINT ["/usr/local/bin/Rscript"]
CMD ["dummy_step.R"]