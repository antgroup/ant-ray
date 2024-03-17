# LLM Applications

A comprehensive guide to building RAG-based LLM applications for production.

- **Blog post**: https://yuque.antfin-inc.com/ray-project/core/lpgtb2lzghki90kr
- **GitHub repository**: https://github.com/alipay/ant-ray/tree/RAG_on_ray
- **Interactive notebook**: https://github.com/alipay/ant-ray/blob/RAG_on_ray/notebooks/RAG_demo_distributed_with_ray.ipynb
- **Ray documentation**: https://docs.ray.io/

In this guide, we will learn how to:

- 💻 Develop a retrieval augmented generation (RAG) based LLM application from scratch.
- 🚀 Scale the major components (load, chunk, embed, index, serve, etc.) in our application.
- ✅ Evaluate different configurations of our application to optimize for both per-component (ex. retrieval_score) and overall performance (quality_score).
- 🔀 Implement LLM hybrid routing approach to bridge the gap b/w OSS and closed LLMs.
- 📦 Serve the application in a highly scalable and available manner.
- 💥 Share the 1st order and 2nd order impacts LLM applications have had on our products.

<br>
<img width="800" src="https://images.ctfassets.net/xjan103pcp94/7FWrvPPlIdz5fs8wQgxLFz/fdae368044275028f0544a3d252fcfe4/image15.png">

## Setup
Some vulnerability setup issues have been documented in [《Ray 构建 RAG Setup & 踩坑》](https://yuque.antfin-inc.com/ray-project/core/lf194i6b9lzsezla).

### API keys
We'll be using [OpenAI](https://platform.openai.com/docs/models/) to access ChatGPT models like `gpt-3.5-turbo`, `gpt-4`, etc.
For AntGroup member, you can contact us via email(chenqixiang.cqx@antgroup.com) or DingTalk(@帕尔) for internal API keys.

### Compute
<details open>
  <summary>Local</summary>
  You could run this on your local laptop but we highly recommend using a setup with access to GPUs.
</details>

<details open>
  <summary>Ant Ray Cluster</summary>
You can contact us with email *chenqixiang.cqx@antgroup.com*.

</details>

### Repository
```bash
git clone https://github.com/alipay/ant-ray/tree/RAG_on_ray .
git config --global user.name <GITHUB-USERNAME>
git config --global user.email <EMAIL-ADDRESS>
```

### Data
Run this bash command to download your documents data into local disk. This may take hours for crawling and downloading, but you can stop at any time, the RAG code works as long as one document is there. And feel free to change the documents url to yours, SHOULD be worked for any text documents.

```bash
export EFS_DIR=/desired/output/directory
wget -e robots=off --recursive --no-clobber --page-requisites \
  --html-extension --convert-links --restrict-file-names=windows \
  --domains docs.ray.io --no-parent --accept=html \
  -P $EFS_DIR https://docs.ray.io/en/master/
```

### Environment

Then set up the environment correctly by specifying the values in your `.env` file,
and installing the dependencies:

```bash
pip install --user -r requirements.txt
export PYTHONPATH=$PYTHONPATH:$PWD
pre-commit install
pre-commit autoupdate
```

### Credentials
Again, for AntGroup members, contact us for internal LLM API keys.
```bash
touch .env
# Add environment variables to .env
OPENAI_API_BASE="https://api.openai.com/v1"
OPENAI_API_KEY=""  # https://platform.openai.com/account/api-keys
DB_CONNECTION_STRING="dbname=postgres user=postgres host=localhost password=postgres"
source .env
```

Now we're ready to go through the [RAG_demo_local_version.ipynb](notebooks/RAG_demo_local_version.ipynb) and [RAG_demo_distributed_with_ray.ipynb](notebooks/RAG_demo_distributed_with_ray.ipynb) interactive notebook to develop and serve our LLM application!

### Learn more
- If your team is investing heavily in developing LLM applications, reach out to us via email(chenqixiang.cqx@antgroup.com) or DingTalk(@帕尔) to learn more about how [Ray](https://github.com/ray-project/ray) can help you scale and productionize everything.
- Learn more about how companies like OpenAI, Netflix, Pinterest, Verizon, Instacart and others leverage Ray and Anyscale for their AI workloads at the [Ray Summit 2023](https://raysummit.anyscale.com/) this Sept 18-20 in San Francisco.
