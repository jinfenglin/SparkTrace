package traceability;


import java.io.Serializable;

public class Link implements Serializable {
    protected Artifact from, to;
    protected boolean hasOrder;

    public Link(Artifact from, Artifact to) {
        this.from = from;
        this.to = to;
        hasOrder = false;
    }


    public Artifact getFrom() {
        return from;
    }

    public void setFrom(Artifact from) {
        this.from = from;
    }

    public Artifact getTo() {
        return to;
    }

    public void setTo(Artifact to) {
        this.to = to;
    }

    public boolean isHasOrder() {
        return hasOrder;
    }

    public void setHasOrder(boolean hasOrder) {
        this.hasOrder = hasOrder;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Link)) return false;

        Link link = (Link) o;

        if (isHasOrder() != link.isHasOrder()) return false;
        boolean identical = getFrom().equals(link.getFrom()) && getTo().equals(link.getTo());
        boolean reversed = getFrom().equals(link.getTo()) && getTo().equals(link.getFrom());
        if (isHasOrder()) {
            return identical;
        } else {
            return identical || reversed;
        }
    }

    @Override
    public int hashCode() {
        int result;
        if (isHasOrder()) {
            result = getFrom().hashCode();
            result = result + getTo().hashCode();
            return 31 * result + (isHasOrder() ? 1 : 0);
        } else {
            result = getFrom().hashCode() + getTo().hashCode();
            return 31 * result + (isHasOrder() ? 1 : 0);
        }
    }

    @Override
    public String toString() {
        return "Link{" +
                ", from=" + from +
                ", to=" + to +
                ", hasOrder=" + hasOrder +
                '}';
    }
}
